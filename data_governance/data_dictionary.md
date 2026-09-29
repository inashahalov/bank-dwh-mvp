# Data Dictionary — Bank DWH MVP

Документ показывает не только техническую схему, но и управляемые
бизнес-атрибуты данных. В реальном проекте владельцы/ответственные лица
уточняются у бизнеса; здесь указаны учебные роли.

## PII и классификация доступа

`full_name` и `birth_date` — персональные данные. В `mart.dim_client` их нет вообще:
- `full_name_hash` — md5(ФИО), только для технических join/сверки, имя не восстановить;
- `initials` — инициалы для читаемости отчётов;
- `age_band` — возрастная группа вместо точной даты рождения (data minimization).

Реальное ФИО и дата рождения лежат отдельно, в `restricted.dim_client_pii`, доступ
к которому есть только у роли `data_steward` (обычный BI-потребитель — роль
`analyst_ro` — не видит эту схему вообще, `REVOKE ALL ... FROM PUBLIC` на уровне БД).
Это и есть механизм access control, а не только декларация в документе.

## Critical Data Elements (CDE)

| Object | Field | Business meaning | Type | Mandatory | Domain / rule | Criticality | PII | Steward role |
|---|---|---|---|---|---|---|---|---|
| dim_client | client_id | Уникальный идентификатор клиента | BIGINT | Да | unique, not null | HIGH | No | Data Steward |
| dim_client | full_name_hash | Псевдонимизированное ФИО (md5) | TEXT | Нет | join-ключ, не PII | LOW | No | Data Steward |
| dim_client | age_band | Возрастная группа | TEXT | Нет | 18-24 .. 65+ | LOW | No | Data Steward |
| dim_client | segment | Клиентский сегмент | TEXT | Нет | retail/premium/private | MEDIUM | No | Data Steward |
| dim_client | kyc_risk_level | Уровень KYC-риска | TEXT | Нет | low/medium/high | HIGH | No | Data Steward |
| restricted.dim_client_pii | full_name | ФИО клиента | TEXT | Да | — | HIGH | **Yes** | Data Steward |
| restricted.dim_client_pii | birth_date | Дата рождения | DATE | Нет | — | HIGH | **Yes** | Data Steward |
| dim_account | account_id | Идентификатор счёта | BIGINT | Да | unique, not null | HIGH | Data Steward |
| dim_account | client_id | Клиент-владелец счёта | BIGINT | Да | exists in dim_client | HIGH | Data Steward |
| dim_account | product_type | Тип банковского продукта | TEXT | Нет | card/deposit/loan/current | MEDIUM | Data Steward |
| fact_transactions | transaction_id | Уникальный идентификатор операции | BIGINT | Да | unique, not null | HIGH | Data Steward |
| fact_transactions | account_id | Счёт операции | BIGINT | Да | exists in dim_account | HIGH | Data Steward |
| fact_transactions | client_id | Клиент операции | BIGINT | Да | exists in dim_client | HIGH | Data Steward |
| fact_transactions | tx_ts | Дата и время операции | TIMESTAMP | Да | not null | HIGH | Data Steward |
| fact_transactions | amount | Сумма операции | NUMERIC | Да | >= 0 | HIGH | Data Steward |
| fact_transactions | tx_type | Тип операции | TEXT | Да | controlled vocabulary | MEDIUM | Data Steward |
| fact_transactions | currency | Валюта операции | TEXT | Да | ISO-like code in production | MEDIUM | Data Steward |
| fact_transactions | source_bank | Источник данных | TEXT | Да | controlled vocabulary | HIGH | Data Steward |
| fact_transactions | load_batch_date | Дата загрузки батча | DATE | Да | not null; freshness | HIGH | Data Engineer |

## DQ dimensions

- **Completeness** — обязательные атрибуты заполнены.
- **Uniqueness** — ключи не дублируются.
- **Validity** — значения соответствуют допустимым диапазонам/справочникам.
- **Consistency** — связи между сущностями не нарушены.
- **Freshness** — ожидаемый ежедневный батч действительно загружен.

## Ownership model

- **Data Owner** — бизнес-владелец домена, определяет смысл и допустимость данных.
- **Data Steward** — поддерживает определения, домены значений, DQ-правила и разбор нарушений.
- **Data Engineer** — отвечает за технический pipeline и исправление дефектов загрузки.
- **Consumer** — аналитик/система, использующая данные.

В учебном проекте роли демонстрационные; конкретный владелец бизнес-данных
не моделируется.
