# Data Lineage

## End-to-end lineage

```text
Bank API
  ├── /clients
  ├── /accounts
  └── /transactions
        │
        ▼
Airflow: extract_raw
        │
        ▼
MinIO / DataLake / raw
  ├── raw/clients/bank=*/dt=*
  ├── raw/accounts/bank=*/dt=*
  └── raw/transactions/bank=*/dt=*
        │
        ▼
Spark: transform.py
  ├── cast / type normalization
  ├── deduplication
  ├── PII masking (full_name -> hash + initials, birth_date -> age_band)
  └── batch watermark
        │
        ├──────────────────────────┐
        ▼                          ▼
Postgres DWH mart          restricted.dim_client_pii
  ├── dim_client (masked)    (реальное ФИО/дата рождения,
  ├── dim_account              доступ только data_steward)
  └── fact_transactions
        │
        ▼
Data Quality layer (ждёт transform_load_mart через ExternalTaskSensor)
  ├── completeness
  ├── uniqueness
  ├── validity
  ├── consistency
  └── freshness
        │
        ├── HIGH FAIL  -> блокирует DAG (severity из dq.rule_catalog)
        └── любой FAIL -> автоматически dq_incident.incidents (OPEN)
        │
        ▼
dq.check_results
        │
        ├── DQ Dashboard
        └── DQ incident / remediation
```

## Field-level example

```text
bank_api.transactions.amount
        ↓
raw/transactions/.../transactions.json
        ↓
Spark CAST -> DECIMAL(18,2)
        ↓
mart.fact_transactions.amount
        ↓
DQ-003: amount >= 0
        ↓
dq.check_results
```

## CDE lineage example

Для `fact_transactions.transaction_id`:

```text
source transaction_id
    ↓
raw transaction_id
    ↓
Spark CAST + dropDuplicates
    ↓
mart.fact_transactions.transaction_id
    ↓
DQ-002 uniqueness
```

Для `fact_transactions.client_id`:

```text
source client_id
    ↓
raw client_id
    ↓
Spark CAST
    ↓
mart.fact_transactions.client_id
    ↓
DQ-006 referential consistency
    ↓
dim_client.client_id
```

## Важное ограничение MVP

Lineage описан вручную как metadata artifact. Это **не автоматический enterprise
lineage scanner** и не заявка на опыт с Collibra/Apache Atlas/DataHub. На собеседовании
корректно говорить: «я реализовал модель lineage и связал её с S2T и DQ metadata».

### Dashboard

`dq.check_results` используется Streamlit Dashboard для отображения результатов DQ, истории качества и нарушений.

```text
dq.check_results
       |
       v
Bank DQ Dashboard
```
