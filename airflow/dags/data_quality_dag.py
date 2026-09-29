"""
DAG: data_quality_checks

Фокус проекта: Data Quality Engineer / Data Steward.

Порядок:
  1. ждёт успешного завершения transform_load_mart за ту же дату
  2. выполняет проверки, пишет результат в dq.check_results
  3. на каждый FAIL заводит запись в dq_incident.incidents (если для этого
     правила и этой даты ещё нет открытого/исторического инцидента — иначе
     ретрай/бэкфилл того же дня плодил бы дубли, см. _open_incident)
  4. падает (блокирует "релиз" батча) только если провалилась HIGH-проверка;
     MEDIUM/LOW логируются и заводят инцидент, но не останавливают pipeline

Scope проверок:
  - dim_client / dim_account — это полный дневной снепшот (TRUNCATE + Spark
    append каждый день, см. transform_load_mart_dag.py), поэтому проверка
    "по всей таблице" здесь и есть проверка "текущего состояния" — отдельный
    date-фильтр тут не нужен и его негде брать (в dim-таблицах нет
    load_batch_date).
  - fact_transactions — append-only, копится история. Проверки, завязанные
    на конкретный батч (validity, completeness полей, consistency ссылок),
    фильтруются по load_batch_date = ds — иначе старый дефект из прошлого
    батча будет FAIL'ить (и плодить инцидент) на каждом следующем прогоне.
  - DQ-002 (uniqueness) — единственное реальное глобальное свойство:
    оставлено без date-фильтра намеренно, скоуп тут не имеет значения,
    потому что transaction_id PRIMARY KEY физически не даёт дублю
    появиться ни в одном батче (см. data_governance/dq_incident_catalog.md).

Проверяемые измерения: completeness, uniqueness, validity, consistency, freshness.

Важно: это учебный DQ framework для синтетического проекта, а не
production-ready enterprise DQ platform.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


# scope="batch" -> SQL содержит один плейсхолдер %s (load_batch_date = %s),
# scope="full"  -> SQL выполняется без параметров (см. docstring выше почему)
CHECKS = [
    {
        "code": "DQ-001", "name": "Client ID is not null", "dimension": "completeness",
        "table": "mart.dim_client", "scope": "full", "threshold": 0,
        "sql": "SELECT COUNT(*) FROM mart.dim_client WHERE client_id IS NULL",
    },
    {
        "code": "DQ-002", "name": "Transaction ID uniqueness", "dimension": "uniqueness",
        "table": "mart.fact_transactions", "scope": "full", "threshold": 0,
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT transaction_id
                FROM mart.fact_transactions
                GROUP BY transaction_id
                HAVING COUNT(*) > 1
            ) d
        """,
    },
    {
        "code": "DQ-003", "name": "Transaction amount is non-negative", "dimension": "validity",
        "table": "mart.fact_transactions", "scope": "batch", "threshold": 0,
        "sql": """
            SELECT COUNT(*)
            FROM mart.fact_transactions
            WHERE load_batch_date = %s AND (amount IS NULL OR amount < 0)
        """,
    },
    {
        "code": "DQ-004", "name": "Allowed transaction type", "dimension": "validity",
        "table": "mart.fact_transactions", "scope": "batch", "threshold": 0,
        "sql": """
            SELECT COUNT(*)
            FROM mart.fact_transactions
            WHERE load_batch_date = %s
              AND (tx_type IS NULL
                   OR tx_type NOT IN ('purchase','transfer_in','transfer_out','withdrawal','fee'))
        """,
    },
    {
        "code": "DQ-005", "name": "Transaction account exists", "dimension": "consistency",
        "table": "mart.fact_transactions", "scope": "batch", "threshold": 0,
        "sql": """
            SELECT COUNT(*)
            FROM mart.fact_transactions f
            LEFT JOIN mart.dim_account a ON a.account_id = f.account_id
            WHERE f.load_batch_date = %s AND (f.account_id IS NULL OR a.account_id IS NULL)
        """,
    },
    {
        "code": "DQ-006", "name": "Transaction client exists", "dimension": "consistency",
        "table": "mart.fact_transactions", "scope": "batch", "threshold": 0,
        "sql": """
            SELECT COUNT(*)
            FROM mart.fact_transactions f
            LEFT JOIN mart.dim_client c ON c.client_id = f.client_id
            WHERE f.load_batch_date = %s AND (f.client_id IS NULL OR c.client_id IS NULL)
        """,
    },
    {
        "code": "DQ-007", "name": "Account client exists", "dimension": "consistency",
        "table": "mart.dim_account", "scope": "full", "threshold": 0,
        "sql": """
            SELECT COUNT(*)
            FROM mart.dim_account a
            LEFT JOIN mart.dim_client c ON c.client_id = a.client_id
            WHERE a.client_id IS NULL OR c.client_id IS NULL
        """,
    },
    {
        # load_batch_date проставляет сам Spark (F.lit(ds)) — по построению
        # никогда не NULL, так что фильтровать эту проверку по load_batch_date
        # бессмысленно (получится "WHERE load_batch_date=%s AND ... IS NULL" —
        # всегда 0 строк, проверка превращается в фикцию). Это структурный
        # инвариант загрузчика, не "дефект одного батча" — поэтому full-scope
        # тут правильный выбор, а не ошибка того же рода, что была в DQ-003..006.
        "code": "DQ-008", "name": "Batch date is present", "dimension": "completeness",
        "table": "mart.fact_transactions", "scope": "full", "threshold": 0,
        "sql": """
            SELECT COUNT(*)
            FROM mart.fact_transactions
            WHERE load_batch_date IS NULL
        """,
    },
    {
        "code": "DQ-009", "name": "Transaction timestamp is present", "dimension": "completeness",
        "table": "mart.fact_transactions", "scope": "batch", "threshold": 0,
        "sql": """
            SELECT COUNT(*)
            FROM mart.fact_transactions
            WHERE load_batch_date = %s AND tx_ts IS NULL
        """,
    },
    {
        # freshness проверяется как обычное "нарушение": metric=1 значит
        # "батча за эту дату нет" — та же модель metric<=threshold, что и
        # у остальных проверок; фактическое число строк не теряется, оно
        # уходит в details (см. record()), а не в metric_value/threshold_value
        "code": "DQ-010", "name": "Daily batch has data", "dimension": "freshness",
        "table": "mart.fact_transactions", "scope": "batch", "threshold": 0,
        "sql": """
            SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
            FROM mart.fact_transactions
            WHERE load_batch_date = %s
        """,
    },
]


# Row-level предикаты для quarantine (алиас таблицы: f). Только для правил,
# нарушение которых привязано к конкретной строке fact_transactions.
# DQ-001/007 (dim-таблицы), DQ-002 (глобальный PK) и DQ-010 (freshness)
# строк для карантина не имеют.
QUARANTINE_WHERE = {
    "DQ-003": "f.amount IS NULL OR f.amount < 0",
    "DQ-004": ("f.tx_type IS NULL OR f.tx_type NOT IN "
               "('purchase','transfer_in','transfer_out','withdrawal','fee')"),
    "DQ-005": ("f.account_id IS NULL OR NOT EXISTS "
               "(SELECT 1 FROM mart.dim_account a WHERE a.account_id = f.account_id)"),
    "DQ-006": ("f.client_id IS NULL OR NOT EXISTS "
               "(SELECT 1 FROM mart.dim_client c WHERE c.client_id = f.client_id)"),
    "DQ-009": "f.tx_ts IS NULL",
}


def _load_active_rules(cur) -> dict:
    cur.execute("SELECT rule_code, criticality, owner_role FROM dq.rule_catalog WHERE active = TRUE")
    return {code: {"criticality": crit, "owner_role": owner} for code, crit, owner in cur.fetchall()}


def _open_incident(cur, ds, check, severity, owner_role, metric):
    # Идемпотентность: ретрай/бэкфилл того же (rule_code, run_date) не должен
    # плодить второй OPEN-инцидент на один и тот же обнаруженный дефект.
    # Новый рабочий день с тем же провалившимся правилом — это ЗАКОНОМЕРНО
    # новая запись (данные за 27.09 и за 28.09 — разные партии), поэтому
    # дедуп ограничен run_date, а не "пока правило вообще не закрыто".
    cur.execute(
        "SELECT incident_id FROM dq_incident.incidents WHERE rule_code = %s AND run_date = %s LIMIT 1",
        (check["code"], ds),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        """
        INSERT INTO dq_incident.incidents
        (run_date, rule_code, severity, dimension, object_name, status, owner_role, description)
        VALUES (%s, %s, %s, %s, %s, 'OPEN', %s, %s)
        """,
        (
            ds, check["code"], severity, check["dimension"], check["table"], owner_role,
            f"Auto-opened by data_quality_checks: {metric} violation(s), threshold {check['threshold']}",
        ),
    )
    cur.execute(
        "SELECT incident_id FROM dq_incident.incidents WHERE rule_code = %s AND run_date = %s LIMIT 1",
        (check["code"], ds),
    )
    return cur.fetchone()[0]


def _quarantine(cur, ds, check, incident_id):
    """Копирует строки-нарушители батча ds в quarantine (идемпотентно по
    (incident_id, transaction_id)). Возвращает список transaction_id."""
    where = QUARANTINE_WHERE.get(check["code"])
    if not where:
        return []
    cur.execute(
        f"""
        INSERT INTO dq_incident.quarantine_transactions
        (incident_id, transaction_id, account_id, client_id, tx_ts,
         amount, tx_type, source_bank, reason)
        SELECT %s, f.transaction_id, f.account_id, f.client_id, f.tx_ts,
               f.amount, f.tx_type, f.source_bank, %s
        FROM mart.fact_transactions f
        WHERE f.load_batch_date = %s AND ({where})
          AND NOT EXISTS (
              SELECT 1 FROM dq_incident.quarantine_transactions q
              WHERE q.incident_id = %s AND q.transaction_id = f.transaction_id)
        """,
        (incident_id, f"{check['code']}: {check['name']}", ds, incident_id),
    )
    cur.execute(
        "SELECT transaction_id FROM dq_incident.quarantine_transactions WHERE incident_id = %s",
        (incident_id,),
    )
    return [r[0] for r in cur.fetchall()]


def run_checks(ds, **_):
    hook = PostgresHook(postgres_conn_id="mart_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    rules = _load_active_rules(cur)
    blocking_failures = []
    all_failures = []
    quarantined_ids = set()

    for check in CHECKS:
        if check["code"] not in rules:
            # правило выключено (active=FALSE в dq.rule_catalog) — пропускаем,
            # это и есть смысл каталога, а не просто справочный текст
            continue

        params = (ds,) if check["scope"] == "batch" else ()
        cur.execute(check["sql"], params)
        metric = cur.fetchone()[0]
        status = "PASS" if metric <= check["threshold"] else "FAIL"

        cur.execute(
            """
            INSERT INTO dq.check_results
            (run_date, rule_code, rule_name, dimension, table_name,
             metric_value, threshold_value, status, details)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (ds, check["code"], check["name"], check["dimension"], check["table"],
             metric, check["threshold"], status,
             f"Observed violations: {metric}; allowed: {check['threshold']}"),
        )

        if status == "FAIL":
            severity = rules[check["code"]]["criticality"]
            owner_role = rules[check["code"]]["owner_role"]
            incident_id = _open_incident(cur, ds, check, severity, owner_role, metric)
            quarantined_ids.update(_quarantine(cur, ds, check, incident_id))
            all_failures.append(f"{check['code']} ({severity}): {metric} violation(s)")
            if severity == "HIGH":
                blocking_failures.append(check["code"])

    # Удаляем из витрины только ПОСЛЕ всех проверок, чтобы порядок правил
    # не влиял на результаты. Строки остаются в dq_incident.quarantine_transactions.
    # Тот же прогон всё равно FAIL'ится (HIGH); повторный запуск (RECHECK)
    # видит чистый батч и даёт PASS.
    if quarantined_ids:
        cur.execute(
            "DELETE FROM mart.fact_transactions WHERE load_batch_date = %s AND transaction_id = ANY(%s)",
            (ds, list(quarantined_ids)),
        )
        print(f"Quarantine: перенесено {len(quarantined_ids)} строк(и) из mart.fact_transactions")

    conn.commit()
    cur.close()
    conn.close()

    if all_failures:
        print("DQ failures (заведены/сверены с dq_incident.incidents): " + "; ".join(all_failures))
    if blocking_failures:
        raise AirflowException(
            "DQ-гейт заблокирован: провалены HIGH-проверки " + ", ".join(blocking_failures)
        )
    print(f"Критических (HIGH) нарушений нет для {ds}")


default_args = {
    "owner": "data-quality",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="data_quality_checks",
    description="Data Quality checks for bank DWH mart",
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["data-quality", "data-governance", "steward"],
) as dag:

    wait_for_mart_load = ExternalTaskSensor(
        task_id="wait_for_transform_load_mart",
        external_dag_id="transform_load_mart",
        external_task_id="spark_transform_to_mart",
        allowed_states=["success"],
        timeout=1800,
        poke_interval=30,
        mode="reschedule",
    )

    dq = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_checks,
        op_kwargs={"ds": "{{ ds }}"},
    )

    wait_for_mart_load >> dq
