# Bank Data Quality & Data Governance MVP

Проект на банковском домене: контур **Data Quality / Data Governance / Data Steward** вокруг небольшого DWH.
Данные синтетические, проект не является production-системой и не отражает работу с данными конкретного банка.

## Как это выглядит

**Чистый батч:** 10 из 10 проверок PASS.

<img src="docs/01-dashboard-pass.png" alt="Dashboard: все проверки PASS" width="900">

**Плохие данные:** те же проверки ловят 4 нарушения, Quality rate падает до 60%.

<img src="docs/02-dashboard-fail.png" alt="Dashboard: 4 нарушения" width="900">

**Инциденты открываются автоматически** (по одному на каждое нарушенное правило), строки-нарушители уходят в карантин:

<img src="docs/04-incidents-open.png" alt="Четыре открытых DQ-инцидента" width="900">

<img src="docs/03-quarantine.png" alt="Строки в карантине" width="420">

**После исправления** повторная проверка даёт PASS, инциденты переведены в `RESOLVED` (статусы после `OPEN` пока меняются вручную):

<img src="docs/05-incidents-resolved.png" alt="Инциденты RESOLVED" width="900">

## Что реализовано

- DQ-правила по измерениям Completeness / Uniqueness / Validity / Consistency / Freshness, критичность HIGH / MEDIUM / LOW хранится в `dq.rule_catalog` и влияет на поведение pipeline;
- контроль ссылочной целостности на уровне DQ (в витрине нет FK);
- журнал результатов `dq.check_results`, автоматическое открытие инцидентов `dq_incident.incidents`;
- **HIGH-нарушение останавливает DAG**, MEDIUM / LOW заводят инцидент, но pipeline продолжается;
- **автоматический карантин** строк-нарушителей в `dq_incident.quarantine_transactions` и удаление их из витрины;
- дубли по бизнес-ключу при загрузке не теряются молча: все копии сохраняются в `s3a://datalake/reject/…`, количество пишется в лог;
- Data Dictionary с PII-классификацией, Critical Data Elements, Source-to-Target Mapping, Data Lineage;
- PII в отдельной схеме `restricted`, разграничение доступа ролями `analyst_ro` и `data_steward`;
- Streamlit-дашборд (метрики считаются по последнему результату каждого правила).

## Архитектура

```text
API simulator ─► Airflow ─► MinIO (raw) ─► Spark ─► PostgreSQL (mart + restricted)
                                                          │
                                                          ▼
                                                    DQ checks (Airflow)
                                      ┌───────────────────┼───────────────────┐
                                      ▼                   ▼                   ▼
                              dq.check_results   dq_incident.incidents   quarantine
                                      │
                                      ▼
                              Streamlit dashboard
```

Три DAG'а с явными зависимостями: `extract_raw` → `transform_load_mart` → `data_quality_checks`
(каждый ждёт предыдущий через `ExternalTaskSensor` с той же логической датой).
Spark-драйвер запускается в Airflow-контейнере (`deploy_mode=client`) и подключается к `spark-master:7077`.

## DQ-правила

| Код | Правило | Измерение | Критичность | Карантин строк |
|---|---|---|---|---|
| DQ-001 | `client_id` не NULL (`dim_client`) | completeness | HIGH | нет (dim) |
| DQ-002 | `transaction_id` уникален | uniqueness | HIGH | нет |
| DQ-003 | `amount >= 0` | validity | HIGH | да |
| DQ-004 | `tx_type` из согласованного домена | validity | MEDIUM | да |
| DQ-005 | счёт транзакции существует | consistency | HIGH | да |
| DQ-006 | клиент транзакции существует | consistency | HIGH | да |
| DQ-007 | клиент счёта существует | consistency | HIGH | нет (dim) |
| DQ-008 | `load_batch_date` заполнена | completeness | HIGH | нет |
| DQ-009 | `tx_ts` заполнен | completeness | HIGH | да (условие заложено) |
| DQ-010 | за дату есть загруженный батч | freshness | HIGH | нет |

Полное описание: [`data_governance/dq_rule_catalog.md`](data_governance/dq_rule_catalog.md).

## Жизненный цикл инцидента

```text
BAD DATA → DQ CHECK → INCIDENT (OPEN) → QUARANTINE → исправление в источнике → RECHECK → PASS → RESOLVED
```

| Шаг | Как сейчас |
|---|---|
| Проверка, FAIL, открытие инцидента, карантин | автоматически (`data_quality_dag.py`) |
| Статусы после `OPEN` (`TRIAGED`, `RESOLVED`, `ACCEPTED`), `root_cause` | вручную, SQL-примеры в `data_governance/incident_demo.sql` |

Инцидент не дублируется: уникальность по `(rule_code, run_date)`.

## Запуск

Нужны Docker и Docker Compose. Первая сборка занимает несколько минут (образы Airflow и Spark большие).

```bash
git clone https://github.com/inashahalov/bank-dwh-mvp.git
cd bank-dwh-mvp
docker compose up -d --build
docker compose ps          # postgres и minio healthy, airflow-init Exited (0)
```

DAG'и после первого старта выключены. Включить их (подождав ~1 минуту после старта):

```bash
docker compose exec airflow-scheduler airflow dags unpause extract_raw
docker compose exec airflow-scheduler airflow dags unpause transform_load_mart
docker compose exec airflow-scheduler airflow dags unpause data_quality_checks
```

Через 2–3 минуты все три прогона должны быть `success`
(`docker compose exec airflow-scheduler airflow dags list-runs -d data_quality_checks`).

| Сервис | Адрес | Доступ (демо) |
|---|---|---|
| Airflow | http://localhost:8081 | `admin` / `admin` |
| DQ Dashboard | http://localhost:8501 | — |
| Spark UI | http://localhost:8080 | — |
| MinIO | http://localhost:9001 | `minioadmin` / `minioadmin` |
| PostgreSQL | `localhost:5432`, БД `mart` | `mart` / `mart` |
| API simulator | http://localhost:8000/docs | — |

Все учётные данные демонстрационные, только для локального запуска.

Остановка и очистка: `docker compose down -v`.

## Сценарий: плохие данные → инцидент → карантин → RECHECK

`scripts/inject_bad_data.sql` добавляет 4 заведомо плохие транзакции (ID от `9000000000001`):
отрицательная сумма (DQ-003), недопустимый тип (DQ-004), несуществующий счёт (DQ-005), несуществующий клиент (DQ-006).
Дата батча в скрипте зашита (`2026-09-28`), поэтому подставляется дата последнего загруженного батча:

```bash
D=$(docker compose exec -T postgres psql -U mart -d mart -At -c "select max(load_batch_date) from mart.fact_transactions")
sed "s/2026-09-28/$D/g" scripts/inject_bad_data.sql | docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U mart -d mart
docker compose exec airflow-scheduler airflow tasks clear data_quality_checks -s $D -e $D -y
```

Через минуту: на дашборде 4 нарушения, `Open incidents = 4`, в карантине 4 строки:

```bash
docker compose exec postgres psql -U mart -d mart -c "select q.quarantine_id, i.rule_code, q.transaction_id, q.reason from dq_incident.quarantine_transactions q join dq_incident.incidents i using (incident_id) order by 1;"
```

Строки из витрины уже удалены. RECHECK: снова `tasks clear data_quality_checks` с той же датой, все проверки PASS.
Закрытие инцидентов и `scripts/cleanup_bad_data.sql` (удаляет остатки тестовых строк) описаны в `data_governance/incident_demo.sql`.

Нарушение DQ-002 (дубль `transaction_id`) вставкой не воспроизвести: `transaction_id` это PRIMARY KEY, БД отклоняет такую строку раньше DQ.
Так же защищены DQ-008 и DQ-009 (`NOT NULL`). DQ-правила при этом нужны как второй рубеж на случай изменения DDL.

## Разграничение доступа

PII (ФИО, дата рождения) лежит в `restricted.dim_client_pii`. В `mart.dim_client` ФИО хранится только в виде хэша.

```bash
docker compose exec -e PGPASSWORD=analyst_ro postgres psql -U analyst_ro -d mart -c "select * from restricted.dim_client_pii limit 1"
# ожидается: permission denied for schema restricted
docker compose exec -e PGPASSWORD=data_steward postgres psql -U data_steward -d mart -c "select * from restricted.dim_client_pii limit 1"
# ожидается: строка с данными
```

## Проектные решения

- **Идемпотентность:** факт перезаливается по батчу (`DELETE` по `load_batch_date`), измерения пересоздаются целиком.
- **Два рубежа защиты:** PK / `NOT NULL` в БД, затем DQ-правила.
- **Критичность влияет на код:** HIGH останавливает DAG, MEDIUM / LOW только открывают инцидент.
- **Карантин не скрывает дефект:** строки копируются с причиной и ссылкой на инцидент, удаляются из витрины после всех проверок (порядок правил не влияет на результат).
- **Дубли не теряются молча:** все копии уходят в `reject/`, в лог пишется число дублирующихся ключей.
- **Метрики дашборда** берутся по последнему результату каждой пары `(run_date, rule_code)`; весь журнал доступен в разделе «Журнал всех прогонов».

## Ограничения

Учебный MVP, некоторые места упрощены сознательно:

- данные синтетические и небольшие (тысячи строк);
- проверки идут **после** загрузки в витрину; правильнее по схеме staging → validate → publish;
- измерения пересоздаются каждый день: нет истории (SCD2) и суррогатных ключей;
- карантин убирает строки из витрины до следующей загрузки батча, исправлять нужно источник;
- дубли складываются в `reject/` в MinIO, но отдельного DQ-правила по этому слою пока нет;
- ФИО маскируется хэшем `md5` без соли; в продукте нужен HMAC с секретом или токенизация;
- `age_band` считается от текущей даты, а не от даты батча, поэтому бэкфилл не воспроизводим;
- `check_results` дописывается при каждом перезапуске DQ (журнал); сводные метрики считаются поверх него;
- статусы инцидента после `OPEN` и `root_cause` заполняются вручную;
- S2T и lineage это metadata-артефакты, которые ведутся руками, а не результат сканера (Collibra, DataHub и т. п.);
- в логах Airflow есть предупреждения: устаревший `PostgresOperator` и пустой Fernet-ключ (для демо допустимо).

## Структура

```text
bank-dwh-mvp/
├── airflow/dags/            extract_raw, transform_load_mart, data_quality_checks
├── api_simulator/           источник данных
├── spark/jobs/transform.py  raw → mart, маскирование PII, reject дублей
├── dashboard/               Streamlit DQ Dashboard
├── mart/init.sql            схемы, таблицы, роли, каталог правил
├── data_governance/         словарь данных, каталог правил, S2T, lineage, сценарии инцидентов
├── scripts/                 inject_bad_data.sql, cleanup_bad_data.sql
├── docs/                    скриншоты
├── docker-compose.yml
└── DOCKER_RUN.md
```

## Документация

- [`data_governance/data_dictionary.md`](data_governance/data_dictionary.md): словарь данных, PII, CDE
- [`data_governance/dq_rule_catalog.md`](data_governance/dq_rule_catalog.md): каталог DQ-правил
- [`data_governance/source_to_target_mapping.md`](data_governance/source_to_target_mapping.md): S2T
- [`data_governance/data_lineage.md`](data_governance/data_lineage.md): lineage
- [`data_governance/dq_incident_catalog.md`](data_governance/dq_incident_catalog.md): каталог сценариев инцидентов
- [`data_governance/incident_demo.sql`](data_governance/incident_demo.sql), [`generate_dq_incidents.sql`](data_governance/generate_dq_incidents.sql): демонстрация инцидентов
- [`DOCKER_RUN.md`](DOCKER_RUN.md): запуск подробнее

Пример сквозного CDE: `bank_api.transactions.amount → mart.fact_transactions.amount → DQ-003 → dashboard`.

## Что можно развивать

1. Проверки до публикации в витрину (staging → validate → publish).
2. DQ-правило и инцидент по `reject/`-слою.
3. SCD2 для измерений, суррогатные ключи.
4. Правила целиком из `dq.rule_catalog` (SQL и пороги в БД, а не в коде DAG'а).
5. dbt tests или Great Expectations вместо собственного SQL-фреймворка.
6. Псевдонимизация через HMAC, автоматическая смена статусов инцидентов.
7. SLA / SLO качества данных.

## Автор

Илья Нашахалов
