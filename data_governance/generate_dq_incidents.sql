-- Synthetic DQ incident generator.
-- Run only against the demo database.
-- Запуск: psql -h localhost -U mart -d mart -v batch_date=2026-09-27 -f data_governance/generate_dq_incidents.sql
-- batch_date = ds того прогона DAG'ов, который хотите проверить (обычно вчерашняя дата, см. DOCKER_RUN.md).
-- Раньше стояло CURRENT_DATE, а scheduler запускает DQ за ds = вчера — дефекты не попадали в проверку.
-- source_bank использует реальные значения проекта (vtb/sber/rshb), а не
-- произвольные — раньше здесь был 'BANK_A', не совпадавший ни с одним
-- значением, которое реально пишет api_simulator/Spark.

-- 1) Completeness
INSERT INTO mart.fact_transactions
(transaction_id, account_id, client_id, tx_ts, amount, tx_type, currency, source_bank, load_batch_date)
VALUES
(900001, 1001, NULL, CURRENT_TIMESTAMP, 1500.00, 'purchase', 'RUB', 'vtb', :'batch_date'::date);

-- 2) Validity
INSERT INTO mart.fact_transactions
(transaction_id, account_id, client_id, tx_ts, amount, tx_type, currency, source_bank, load_batch_date)
VALUES
(900002, 1001, 2001, CURRENT_TIMESTAMP, -500.00, 'purchase', 'RUB', 'vtb', :'batch_date'::date);

-- 3) Uniqueness.
-- transaction_id — PRIMARY KEY, поэтому "просто вставить дубль" физически
-- невозможно: Postgres откажет unique_violation раньше, чем до строки
-- дойдёт DQ-проверка DQ-002. Раньше этот блок либо падал с ошибкой, либо
-- (если запускался не по порядку) не демонстрировал ничего.
-- Ловим unique_violation явно и заводим на этом инцидент DUPLICATION —
-- это честная демонстрация: PK — первый рубеж защиты, DQ-002 — второй
-- (на случай если где-то в пайплайне PK обошли, например через TRUNCATE+load).
DO $$
DECLARE
    dup_id BIGINT;
BEGIN
    SELECT MIN(transaction_id) INTO dup_id FROM mart.fact_transactions;

    BEGIN
        INSERT INTO mart.fact_transactions
        (transaction_id, account_id, client_id, tx_ts, amount, tx_type, currency, source_bank, load_batch_date)
        SELECT transaction_id, account_id, client_id, tx_ts, amount, tx_type, currency, source_bank, load_batch_date
        FROM mart.fact_transactions
        WHERE transaction_id = dup_id;
    EXCEPTION WHEN unique_violation THEN
        INSERT INTO dq_incident.incidents
        (rule_code, severity, dimension, object_name, status, root_cause, owner_role, description)
        VALUES
        ('DQ-002', 'HIGH', 'uniqueness', 'mart.fact_transactions', 'OPEN',
         'DUPLICATION', 'Data Steward',
         format('PK перехватил попытку дубля transaction_id=%s раньше, чем сработал DQ-002', dup_id));
    END;
END $$;

-- 4) Consistency
INSERT INTO mart.fact_transactions
(transaction_id, account_id, client_id, tx_ts, amount, tx_type, currency, source_bank, load_batch_date)
VALUES
(900004, 99999999, 2001, CURRENT_TIMESTAMP, 100.00, 'purchase', 'RUB', 'vtb', :'batch_date'::date);

-- После запуска: Airflow -> data_quality_checks.
-- Инциденты по пунктам 1, 2, 4 заведутся автоматически DAG'ом
-- (data_quality_dag.py -> _open_incident) на ближайшем прогоне,
-- пункт 3 — заведён этим скриптом напрямую через перехват unique_violation.
