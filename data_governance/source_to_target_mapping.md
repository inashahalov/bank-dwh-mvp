# Source-to-Target Mapping (S2T)

S2T связывает физическое поле источника с полем DWH, фиксируя преобразование,
бизнес-смысл и контроль качества.

| Source | Source field | Target | Target field | Transformation | Business rule | DQ | CDE |
|---|---|---|---|---|---|---|---|
| bank_api.clients | client_id | mart.dim_client | client_id | CAST -> BIGINT | ID клиента | DQ-001 | Yes |
| bank_api.clients | full_name | restricted.dim_client_pii | full_name | direct (ПДн) | Доступ только data_steward | — | No |
| bank_api.clients | full_name | mart.dim_client | full_name_hash | md5(full_name) | Псевдонимизация для join | — | No |
| bank_api.clients | full_name | mart.dim_client | initials | regexp initials | Инициалы для отчётов | — | No |
| bank_api.clients | birth_date | restricted.dim_client_pii | birth_date | CAST -> DATE (ПДн) | Доступ только data_steward | — | No |
| bank_api.clients | birth_date | mart.dim_client | age_band | возрастная группа | Data minimization | — | No |
| bank_api.clients | segment | mart.dim_client | segment | direct | Сегмент клиента | — | No |
| bank_api.clients | kyc_risk_level | mart.dim_client | kyc_risk_level | direct | Уровень KYC-риска | — | Yes |
| bank_api.accounts | account_id | mart.dim_account | account_id | CAST -> BIGINT | ID счёта | — | Yes |
| bank_api.accounts | client_id | mart.dim_account | client_id | CAST -> BIGINT | Владелец должен существовать | DQ-007 | Yes |
| bank_api.accounts | product_type | mart.dim_account | product_type | domain mapping | Тип продукта | — | No |
| bank_api.transactions | transaction_id | mart.fact_transactions | transaction_id | CAST -> BIGINT | ID операции уникален | DQ-002 | Yes |
| bank_api.transactions | account_id | mart.fact_transactions | account_id | CAST -> BIGINT | Счёт существует | DQ-005 | Yes |
| bank_api.transactions | client_id | mart.fact_transactions | client_id | CAST -> BIGINT | Клиент существует | DQ-006 | Yes |
| bank_api.transactions | tx_ts | mart.fact_transactions | tx_ts | CAST -> TIMESTAMP | Время операции обязательно | DQ-009 | Yes |
| bank_api.transactions | tx_type | mart.fact_transactions | tx_type | domain validation | Controlled vocabulary | DQ-004 | No |
| bank_api.transactions | amount | mart.fact_transactions | amount | CAST -> DECIMAL(18,2) | Сумма >= 0 | DQ-003 | Yes |
| bank_api.transactions | currency | mart.fact_transactions | currency | direct | Валюта операции | — | Yes |
| Airflow | execution_date | mart.fact_transactions | load_batch_date | CAST -> DATE | Watermark загрузки | DQ-008 | Yes |

## Как читать S2T

**Источник → поле → преобразование → бизнес-правило → DQ-контроль.**

Это позволяет на собеседовании показать, что Data Steward работает не только
с SQL-проверками, но и с семантикой данных и их преобразованием по пути в DWH.
