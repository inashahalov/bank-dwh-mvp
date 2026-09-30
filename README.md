# Bank Data Quality & Data Governance MVP

Учебный pet-project на банковском домене, демонстрирующий практическую реализацию **Data Quality (DQ)** и отдельных элементов **Data Governance** вокруг DWH-контура.

Проект построен на **синтетических данных** и предназначен для демонстрации технических и аналитических навыков. Он **не является production-системой**, не имитирует промышленный DWH конкретного банка и не заявляет учебную реализацию как коммерческий опыт.

## Что демонстрирует проект

Основной акцент проекта — не на построении промышленного DWH, а на контроле качества данных и работе с метаданными вокруг DWH-контура.

В проекте реализованы:

* каталог DQ-правил;
* автоматическая проверка качества данных;
* классификация нарушений по severity;
* блокировка успешного завершения DQ-пайплайна при критических нарушениях;
* регистрация DQ-инцидентов;
* lifecycle обработки инцидента;
* карантин проблемных записей;
* повторная проверка после исправления;
* Source-to-Target Mapping (S2T);
* Data Lineage;
* Data Dictionary;
* выделение CDE / PII;
* базовое разграничение доступа на уровне PostgreSQL;
* DQ Dashboard на Streamlit.

Упрощённый сценарий работы:

```text
Bank API Simulator
        ↓
   Raw / MinIO
        ↓
  Spark transformation
        ↓
 PostgreSQL mart
        ↓
    DQ checks
        ↓
   PASS / FAIL
        ↓
    Incident
        ↓
   Quarantine
        ↓
     Recheck
        ↓
 RESOLVED / ACCEPTED
        ↓
    Dashboard
```

---

## Ключевые возможности

### Data Quality

Реализован каталог из 10 DQ-правил, охватывающих несколько измерений качества данных:

* Completeness;
* Uniqueness;
* Validity;
* Consistency;
* Freshness.

Правила хранятся в `dq.rule_catalog` и загружаются DQ DAG динамически.

### DQ Incident Management

При нарушении DQ-правила создаётся DQ-инцидент.

Реализованный lifecycle:

```text
OPEN
  ↓
TRIAGED
  ↓
IN_REMEDIATION
  ↓
RECHECK
  ↓
RESOLVED / ACCEPTED
```

Для инцидента фиксируются:

* сценарий;
* DQ rule;
* severity;
* дата обнаружения;
* статус;
* категория root cause;
* описание;
* batch / run;
* результаты повторной проверки.

### Quarantine

Для row-level нарушений реализован механизм карантина.

Проблемные записи помещаются в:

```text
dq_incident.quarantine_transactions
```

После этого некорректные записи удаляются из целевой таблицы:

```text
mart.fact_transactions
```

Обработка реализована идемпотентно: одна и та же запись не должна повторно попадать в карантин для одного и того же инцидента.

### Critical DQ violations

Для критических нарушений используется `severity = HIGH`.

При обнаружении критического нарушения DQ-task завершается с ошибкой, что позволяет показать сценарий:

```text
DQ violation
      ↓
    FAIL
      ↓
   Incident
      ↓
 Quarantine
      ↓
 Remediation
      ↓
   Recheck
      ↓
  RESOLVED
```

---

# DQ Rule Catalog

| Код    | Проверка                                         | Dimension    | Severity |
| ------ | ------------------------------------------------ | ------------ | -------- |
| DQ-001 | `client_id` не должен быть NULL                  | Completeness | HIGH     |
| DQ-002 | `transaction_id` должен быть уникальным          | Uniqueness   | HIGH     |
| DQ-003 | `amount` не может быть отрицательным             | Validity     | HIGH     |
| DQ-004 | `tx_type` должен входить в допустимый справочник | Validity     | MEDIUM   |
| DQ-005 | `account_id` должен существовать                 | Consistency  | HIGH     |
| DQ-006 | `client_id` должен существовать                  | Consistency  | HIGH     |
| DQ-007 | Клиент счета должен существовать                 | Consistency  | HIGH     |
| DQ-008 | Дата batch должна присутствовать                 | Completeness | HIGH     |
| DQ-009 | Timestamp транзакции должен присутствовать       | Completeness | HIGH     |
| DQ-010 | Ежедневный batch должен содержать данные         | Freshness    | HIGH     |

Результаты проверок сохраняются в:

```text
dq.check_results
```

Для каждой проверки фиксируются:

* DQ rule;
* дата / batch;
* PASS / FAIL;
* количество проверенных записей;
* количество нарушений;
* severity.

---

# Архитектура

```text
                    ┌─────────────────┐
                    │    Bank API     │
                    │    Simulator    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Airflow     │
                    │  extract_raw    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      MinIO      │
                    │    raw layer    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      Spark      │
                    │ transformation  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │      mart       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   Data Quality  │
                    │       DAG       │
                    └───────┬─┬───────┘
                            │ │
               ┌────────────┘ └──────────────┐
               ▼                             ▼
      ┌─────────────────┐          ┌──────────────────┐
      │   DQ Results    │          │    Incidents     │
      │                 │          │  + Quarantine    │
      └────────┬────────┘          └──────────────────┘
               │
               ▼
      ┌─────────────────┐
      │    Streamlit    │
      │    Dashboard    │
      └─────────────────┘
```

Полная схема также представлена в:

```text
pictures/pipeline.svg
```

---

# Data Governance

В проекте реализованы отдельные элементы Data Governance, связанные с DWH-контуром.

## Data Dictionary

Документ:

```text
data_governance/data_dictionary.md
```

Содержит описание основных сущностей и полей проекта.

## Source-to-Target Mapping

Документ:

```text
data_governance/source_to_target_mapping.md
```

S2T содержит:

* source;
* source field;
* target;
* target field;
* transformation;
* business rule;
* DQ rule;
* CDE / PII.

Пример логики:

```text
Source:
transactions.amount

        ↓

Transformation:
CAST(...)

        ↓

Target:
mart.fact_transactions.amount

        ↓

DQ:
DQ-003 amount >= 0
```

S2T также представлен в базе данных:

```text
governance.s2t_mapping
```

## Data Lineage

Основная цепочка:

```text
Bank API
   ↓
Airflow
   ↓
MinIO / Raw
   ↓
Spark
   ↓
PostgreSQL / Mart
   ↓
Data Quality
   ↓
DQ Results / Incidents
   ↓
Dashboard
```

Метаданные lineage хранятся в:

```text
governance.data_lineage
```

Документация:

```text
data_governance/data_lineage.md
```

---

# CDE / PII

В проекте показано базовое разделение аналитических данных и ограниченных данных.

Основные таблицы:

```text
mart.dim_client
mart.dim_account
mart.fact_transactions
```

Отдельная таблица для ограниченных данных:

```text
restricted.dim_client_pii
```

Пример PII:

* `full_name`;
* `birth_date`.

В аналитическом контуре персональные данные не используются непосредственно там, где они не нужны для аналитической задачи.

---

# RBAC

В MVP продемонстрировано базовое разграничение доступа на уровне PostgreSQL.

Используются роли:

```text
analyst_ro
data_steward
```

Упрощённая модель:

```text
analyst_ro
   ↓
аналитические mart-таблицы

data_steward
   ↓
аналитические таблицы
   +
restricted.dim_client_pii
```

Это **не корпоративная IAM/RBAC-платформа**, а демонстрация принципа разделения доступа к данным внутри учебного PostgreSQL-контура.

---

# DQ Dashboard

Для визуализации используется Streamlit.

Dashboard позволяет посмотреть:

* последний batch;
* результаты DQ-проверок;
* PASS / FAIL;
* количество нарушений;
* Quality Rate;
* распределение нарушений по dimension;
* историю запусков;
* каталог DQ-правил;
* DQ-инциденты;
* S2T;
* Data Lineage.

Скриншоты:

```text
docs/01-dashboard-pass.png
docs/02-dashboard-fail.png
docs/03-incidents-resolved.png
docs/03-quarantine.png
```

---

# Демонстрационный сценарий DQ-нарушения

В проекте предусмотрен воспроизводимый сценарий для демонстрации работы DQ-контура.

### 1. Внести некорректные данные

```text
scripts/inject_bad_data.sql
```

Например:

* отрицательный `amount`;
* NULL;
* некорректный `tx_type`;
* отсутствующая ссылка на account/client.

### 2. Запустить DQ-проверку

DQ DAG обнаруживает нарушение.

### 3. Получить FAIL

Критическое нарушение приводит к:

```text
PASS → FAIL
```

### 4. Создать инцидент

Фиксируется DQ Incident.

### 5. Поместить проблемные записи в quarantine

```text
dq_incident.quarantine_transactions
```

### 6. Выполнить remediation

Некорректные записи удаляются из целевой таблицы.

### 7. Выполнить повторную проверку

После исправления выполняется `RECHECK`.

### 8. Закрыть инцидент

При успешной повторной проверке:

```text
RECHECK → RESOLVED
```

Для очистки демонстрационных данных:

```text
scripts/cleanup_bad_data.sql
```

---

# Pipeline

В проекте используются три основных Airflow DAG:

```text
airflow/dags/extract_raw_dag.py
airflow/dags/transform_load_mart_dag.py
airflow/dags/data_quality_dag.py
```

Общая последовательность:

```text
extract_raw
     ↓
transform_load_mart
     ↓
data_quality
```

Spark используется для трансформации данных:

```text
spark://spark-master:7077
```

---

# Технологический стек

| Технология     | Назначение                                  |
| -------------- | ------------------------------------------- |
| Python         | ETL/DQ-логика и автоматизация               |
| SQL            | DQ-проверки, работа с данными и метаданными |
| PostgreSQL     | DWH mart, metadata, DQ results, incidents   |
| Apache Airflow | Оркестрация пайплайнов                      |
| Apache Spark   | Трансформация данных                        |
| MinIO          | Object Storage / raw layer                  |
| Streamlit      | DQ Dashboard                                |
| Docker Compose | Локальное развёртывание                     |
| Git            | Контроль версий                             |

---

# Структура проекта

```text
bank-dwh-mvp/
│
├── api_simulator/
│   ├── app.py
│   └── Dockerfile
│
├── airflow/
│   ├── dags/
│   │   ├── extract_raw_dag.py
│   │   ├── transform_load_mart_dag.py
│   │   └── data_quality_dag.py
│   └── Dockerfile
│
├── dashboard/
│   ├── app.py
│   └── Dockerfile
│
├── data_governance/
│   ├── data_dictionary.md
│   ├── dq_rule_catalog.md
│   ├── dq_incident_catalog.md
│   ├── dq_dashboard.md
│   ├── dq_incident_dashboard_queries.sql
│   ├── source_to_target_mapping.md
│   └── data_lineage.md
│
├── docs/
│   ├── 01-dashboard-pass.png
│   ├── 02-dashboard-fail.png
│   ├── 03-incidents-resolved.png
│   └── 03-quarantine.png
│
├── mart/
│   └── init.sql
│
├── scripts/
│   ├── cleanup_bad_data.sql
│   └── inject_bad_data.sql
│
├── spark/
│   └── jobs/
│       └── transform.py
│
├── pictures/
│   └── pipeline.svg
│
├── docker-compose.yml
├── .env.example
├── DOCKER_RUN.md
└── README.md
```

---

# Быстрый запуск

```bash
git clone https://github.com/inashahalov/bank-dwh-mvp.git
cd bank-dwh-mvp

docker compose up -d --build

docker compose ps
```

После запуска доступны основные компоненты проекта:

* Apache Airflow;
* PostgreSQL;
* MinIO;
* Streamlit Dashboard;
* Spark.

Подробная инструкция:

```text
DOCKER_RUN.md
```

---

# Что демонстрирует проект с точки зрения компетенций

## Data Quality

Основной фокус проекта:

* DQ dimensions;
* DQ rule catalog;
* severity;
* PASS / FAIL;
* quality metrics;
* контроль критических нарушений;
* quarantine;
* remediation;
* recheck;
* resolution.

## Data Governance

Демонстрируются:

* Data Dictionary;
* CDE / PII;
* базовый RBAC;
* Source-to-Target Mapping;
* Data Lineage;
* metadata tables.

## DQ Incident Management

Реализованы:

* регистрация инцидента;
* severity;
* root cause;
* lifecycle;
* remediation;
* quarantine;
* recheck;
* resolution.

## DWH / Data Systems

Проект демонстрирует понимание связанных с DWH задач:

* source-to-target mapping;
* контроль качества данных при загрузке;
* работа с mart;
* SQL;
* зависимости между слоями;
* описание происхождения данных;
* работа с метаданными.

## Data Engineering

Data Engineering используется как поддерживающая часть проекта:

* Airflow;
* Spark;
* MinIO;
* PostgreSQL;
* Docker Compose;
* Python;
* SQL.

Проект **не позиционируется как доказательство production-опыта Data Engineer**.

---

# Ограничения проекта

Это учебный MVP, поэтому в нём отсутствуют или упрощены некоторые production-компоненты:

* используются синтетические данные;
* нет реальных банковских данных;
* нет enterprise Data Catalog;
* нет Collibra / Apache Atlas / DataHub;
* RBAC реализован на уровне PostgreSQL;
* lineage реализован как metadata-модель;
* DQ rules не являются частью корпоративной DQ-платформы;
* нет промышленной системы уведомлений;
* нет интеграции с реальным ITSM;
* нет production SLA/SLO;
* нет реального enterprise IAM;
* проект не заявляет опыт промышленной эксплуатации конкретного банковского DWH.

Цель проекта — показать понимание принципов **Data Quality, Data Governance и работы с данными в DWH-контуре**, а также способность самостоятельно собрать воспроизводимый технический MVP.

---

# Позиционирование

Проект ориентирован прежде всего на следующие направления:

* **Data Quality Engineer**
* **Data Steward**
* **Data Governance / Data Quality Analyst**
* **DWH / Data Systems Analyst**

Data Engineering здесь выступает как техническая основа для реализации DQ-контура, а не как основное позиционирование проекта.

Проект не заменяет коммерческий опыт и не должен рассматриваться как подтверждение production-разработки DWH или Data Engineering.

---

# Автор

**Илья Нашахалов**

IT / Banking / Data Quality / DWH / Data Governance
