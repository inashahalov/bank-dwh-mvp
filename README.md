# Bank Data Quality & Data Governance MVP

Учебный пет-проект для демонстрации навыков **Data Quality Engineer /
Data Steward (Data Governance)** на банковском домене.

Проект намеренно не позиционируется как production DWH и не имитирует
реальный опыт работы с ВТБ/Сбером/РСХБ. Все источники синтетические.

## Что демонстрирует проект

Основной акцент:

- анализ качества данных на выходе DWH;
- формализация DQ-правил с критичностью (HIGH/MEDIUM/LOW), которая реально
  используется в коде, а не только задокументирована;
- классификация проверок по измерениям качества;
- контроль completeness / uniqueness / validity / consistency / freshness;
- контроль referential integrity на уровне DQ, а не только FK;
- журналирование результатов проверок;
- автоматическое заведение инцидентов на каждый FAIL и остановка pipeline
  при HIGH-нарушениях;
- маскирование ПДн (ФИО, дата рождения) и разграничение доступа ролями
  Postgres (`analyst_ro` не видит `restricted`, `data_steward` видит);
- data dictionary с явной PII-классификацией;
- Critical Data Elements (CDE);
- controlled vocabulary / допустимые значения;
- распределение ролей Data Owner / Data Steward / Data Engineer.

Технический pipeline используется как среда, на которой демонстрируется
Data Quality:

```text
[API simulator]
       |
    Airflow
       |
       v
[MinIO / raw]
       |
    Spark
       |
       v
[Postgres mart]  +  [Postgres restricted (PII, только data_steward)]
       |
       v
[Data Quality checks]   (ждёт transform_load_mart через ExternalTaskSensor)
       |
       +----> dq.check_results (PASS/FAIL по каждой проверке)
       |
       +----> dq_incident.incidents (авто-OPEN на каждый FAIL)
       |
       +----> HIGH FAIL -> DAG падает; MEDIUM/LOW -> инцидент, но не блокирует
```

## Архитектура

- **API simulator** — три условных банковских источника.
- **MinIO** — raw DataLake.
- **Airflow** — оркестрация, три DAG'а с явными зависимостями:
  `extract_raw` → `transform_load_mart` → `data_quality_checks`
  (каждый ждёт предыдущий через `ExternalTaskSensor`, гонки нет).
- **Spark** — трансформация raw -> mart, включая маскирование ПДн.
- **Postgres** — DWH mart (`mart`) + изолированная PII-схема (`restricted`).
- **Роли доступа** — `analyst_ro` (только `mart`) и `data_steward` (+ `restricted`).
- **DQ layer** — Airflow DAG с SQL-проверками, критичность из `dq.rule_catalog`
  определяет, блокирует ли нарушение pipeline.
- **Data Governance layer** — словарь данных с PII-классификацией, каталог
  DQ-правил, S2T mapping, lineage.

## DQ checks

### Completeness
- `client_id IS NOT NULL`
- `load_batch_date IS NOT NULL`
- `tx_ts IS NOT NULL`

### Uniqueness
- отсутствие дублей `transaction_id`

### Validity
- `amount >= 0`
- `tx_type` входит в согласованный домен

### Consistency
- `transaction.account_id` существует в `dim_account`
- `transaction.client_id` существует в `dim_client`
- `account.client_id` существует в `dim_client`

### Freshness
- за ожидаемую дату существует загруженный batch

Результаты сохраняются в:

```sql
dq.check_results
```

Каталог правил:

```sql
dq.rule_catalog
```

## Data Governance

В `data_governance/` находятся:

- `data_dictionary.md` — определения ключевых атрибутов;
- `dq_rule_catalog.md` — каталог DQ-правил, критичность и ожидаемый результат.

Особенно важная часть для позиции Data Steward — не сам SQL, а связка:

```text
бизнес-термин
    ↓
data element
    ↓
definition
    ↓
domain / constraint
    ↓
DQ rule
    ↓
measurement
    ↓
incident / remediation
```

## Как запускать

```bash
docker compose up --build -d
```

Airflow:

```text
http://localhost:8081
admin / admin
```

MinIO:

```text
http://localhost:9001
minioadmin / minioadmin
```

Spark UI:

```text
http://localhost:8080
```

После загрузки данных включить DAG'и:

1. `extract_raw`
2. `transform_load_mart`
3. `data_quality_checks`

Проверка разграничения доступа:

```bash
psql -h localhost -U analyst_ro -d mart -c "select * from restricted.dim_client_pii"
# -> permission denied for schema restricted

psql -h localhost -U data_steward -d mart -c "select * from restricted.dim_client_pii"
# -> отдаёт строки
```

## S2T + Data Lineage + DQ Dashboard

Проект дополнен полноценной связкой metadata:

```text
Source
  ↓
Source-to-Target Mapping
  ↓
DWH mart
  ↓
Data Lineage
  ↓
DQ Rules
  ↓
DQ Results
  ↓
Dashboard
```

### Source-to-Target Mapping

`data_governance/source_to_target_mapping.md` фиксирует источник, поле, target,
преобразование, бизнес-правило, DQ rule и признак CDE. В БД тот же metadata
доступен через `governance.s2t_mapping`.

### Data Lineage

`data_governance/data_lineage.md` показывает путь данных:

`Bank API → Airflow → MinIO raw → Spark → Postgres mart → DQ → Dashboard`.

В БД lineage доступен через `governance.data_lineage`.

### Важное для запуска Spark

Airflow-образ содержит Spark client (`spark-submit`) и Java 17. Сам Spark cluster остаётся в отдельных контейнерах `spark-master`/`spark-worker`; `SparkSubmitOperator` запускает driver в Airflow-контейнере в `deploy_mode=client` и подключается к `spark-master:7077`. Это нужно для корректной работы `transform_load_mart`.

Dashboard и Airflow ждут готовности PostgreSQL; Dashboard дополнительно повторяет подключение к БД при кратковременной недоступности на старте.

### DQ Dashboard

Streamlit dashboard запускается на `http://localhost:8501` и показывает:

- PASS/FAIL и Quality Rate;
- качество по измерениям;
- историю качества;
- нарушения;
- DQ rule catalog;
- S2T mapping;
- lineage;
- DQ Incidents (заводятся автоматически, см. `data_quality_dag.py`).

Для собеседования это позволяет пройти по одному CDE от источника до DQ:

`bank_api.transactions.amount → mart.fact_transactions.amount → DQ-003 → dashboard`.


**Что я НЕ доделал и говорю об этом прямо:** переходы инцидента по статусам
после OPEN (TRIAGED → RESOLVED) — ручной процесс, не автоматизирован; lineage
и S2T — metadata-артефакты, которые я сам создал и поддерживаю руками, а не
результат сканера (Collibra/Atlas/DataHub).



**«Сделал pet-проект, в котором самостоятельно реализовал DQ-проверки,
каталог правил, журналирование результатов и документацию ключевых данных
на синтетическом банковском наборе».**

## Структура

```text
bank-dwh-mvp/
├── api_simulator/
├── airflow/
│   └── dags/
│       ├── extract_raw_dag.py
│       ├── transform_load_mart_dag.py
│       └── data_quality_dag.py
├── mart/
│   └── init.sql
├── spark/
│   └── jobs/
│       └── transform.py
├── data_governance/
│   ├── data_dictionary.md
│   ├── dq_rule_catalog.md
│   ├── source_to_target_mapping.md
│   ├── data_lineage.md
│   ├── dq_dashboard.md
│   └── demo_queries.sql
├── dashboard/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
└── README.md
```

## Следующее развитие проекта

Если проект расширять именно под Data Quality / Data Governance, приоритет:

1. **DQ thresholds и severity** — разные пороги для HIGH/MEDIUM/LOW.
2. **Quarantine layer** — складывать плохие записи отдельно от валидных.
3. **DQ dashboard** — динамика качества по источникам и датам.
4. **Source-to-target mapping** — документировать происхождение CDE.
5. **Reference data** — отдельные справочники и проверки их актуальности.
6. **Great Expectations или dbt tests** — заменить/дополнить собственный SQL-framework.
7. **Data lineage** — источник → raw → transform → mart → consumer.
8. **SLA/SLO качества** — например, доля валидных записей и допустимый процент нарушений.


## DQ Incident Management

Проект также демонстрирует жизненный цикл DQ-инцидента.

```text
BAD DATA
   ↓
DQ CHECK
   ↓
INCIDENT OPEN
   ↓
TRIAGE
   ↓
ROOT CAUSE
   ↓
QUARANTINE
   ↓
REMEDIATION
   ↓
RECHECK
   ↓
PASS
   ↓
RESOLVED / ACCEPTED
```

Демонстрационные сценарии находятся в:

```text
data_governance/dq_incident_catalog.md
data_governance/generate_dq_incidents.sql
data_governance/incident_demo.sql
data_governance/dq_incident_dashboard_queries.sql
```

### Примеры дефектов

- обязательный `client_id` отсутствует;
- отрицательная сумма;
- duplicate `transaction_id`;
- отсутствующий `account_id` в справочнике;
- отсутствующий daily batch.

Для каждого сценария определены:

- DQ dimension;
- severity;
- предполагаемая root cause;
- зона ответственности;
- quarantine;
- remediation;
- повторная проверка.


Важно: сценарии синтетические и предназначены для демонстрации процесса,
а не являются реальными производственными инцидентами.
