# DQ Rule Catalog

| Rule | Dimension | Severity | Check | Expected |
|---|---|---|---|---|
| DQ-001 | Completeness | HIGH | client_id is not null | 0 violations |
| DQ-002 | Uniqueness | HIGH | transaction_id has no duplicates | 0 violations |
| DQ-003 | Validity | HIGH | amount is not null and >= 0 | 0 violations |
| DQ-004 | Validity | MEDIUM | tx_type belongs to controlled domain | 0 violations |
| DQ-005 | Consistency | HIGH | transaction.account_id exists | 0 violations |
| DQ-006 | Consistency | HIGH | transaction.client_id exists | 0 violations |
| DQ-007 | Consistency | HIGH | account.client_id exists | 0 violations |
| DQ-008 | Completeness | HIGH | load_batch_date is not null | 0 violations |
| DQ-009 | Completeness | HIGH | tx_ts is not null | 0 violations |
| DQ-010 | Freshness | HIGH | expected daily batch exists | >= 1 row |

## Incident handling

1. DQ DAG фиксирует нарушение в `dq.check_results` — для КАЖДОГО FAIL, независимо от severity.
2. DAG (`data_quality_dag.py`, функция `_open_incident`) автоматически заводит
   запись в `dq_incident.incidents` со статусом `OPEN` и severity из `dq.rule_catalog`.
3. Если severity = HIGH хотя бы у одной проверки — DAG падает (`AirflowException`),
   это и есть блокировка "релиза" батча. MEDIUM/LOW логируются и заводят
   инцидент, но не останавливают pipeline.
4. Дальше (TRIAGED → IN_REMEDIATION → RECHECK → RESOLVED/ACCEPTED) — ручной процесс,
   через `data_governance/incident_demo.sql`; автоматического перевода по статусам
   после OPEN в проекте нет — это осознанная граница MVP, не забытая часть.
5. Data Steward классифицирует дефект: source / mapping / transformation / reference data.
6. Data Engineer устраняет техническую причину, если она находится в pipeline.
7. После исправления выполняется повторная DQ-проверка.
