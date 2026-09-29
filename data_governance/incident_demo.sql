-- ============================================================
-- DQ INCIDENT DEMO
-- ============================================================

-- A. Создать инцидент: отрицательная сумма
INSERT INTO dq_incident.incidents
(rule_code, severity, dimension, object_name, status,
 root_cause, owner_role, description)
VALUES
('DQ-003','HIGH','validity','mart.fact_transactions','OPEN',
 'SOURCE_DATA','Data Steward',
 'Обнаружена транзакция с отрицательной суммой');

-- B. Пример quarantine
-- В production запись переносится/копируется сюда автоматическим процессом.
INSERT INTO dq_incident.quarantine_transactions
(incident_id, transaction_id, account_id, client_id, tx_ts,
 amount, tx_type, source_bank, reason)
SELECT
    1, transaction_id, account_id, client_id, tx_ts,
    amount, tx_type, source_bank,
    'DQ-003: amount < 0'
FROM mart.fact_transactions
WHERE amount < 0;

-- C. Перевести incident в remediation
UPDATE dq_incident.incidents
SET status = 'IN_REMEDIATION'
WHERE incident_id = 1;

-- D. После исправления source/pipeline — повторная DQ-проверка.
UPDATE dq_incident.incidents
SET status = 'RECHECK'
WHERE incident_id = 1;

-- E. После успешного re-check
UPDATE dq_incident.incidents
SET status = 'RESOLVED',
    resolved_at = CURRENT_TIMESTAMP,
    resolution = 'Source record corrected; DQ-003 passed on re-check'
WHERE incident_id = 1;

-- F. Data Steward подтверждает
UPDATE dq_incident.incidents
SET status = 'ACCEPTED'
WHERE incident_id = 1;

-- G. Контроль SLA/открытых инцидентов
SELECT
    incident_id,
    opened_at,
    rule_code,
    severity,
    dimension,
    status,
    root_cause,
    owner_role
FROM dq_incident.incidents
ORDER BY opened_at DESC;
