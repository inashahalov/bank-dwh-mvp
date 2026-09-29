-- KPI 1: открытые инциденты
SELECT COUNT(*) AS open_incidents
FROM dq_incident.incidents
WHERE status NOT IN ('RESOLVED','ACCEPTED');

-- KPI 2: инциденты по root cause
SELECT root_cause, COUNT(*) AS incidents
FROM dq_incident.incidents
GROUP BY root_cause
ORDER BY incidents DESC;

-- KPI 3: инциденты по измерениям качества
SELECT dimension, severity, COUNT(*) AS incidents
FROM dq_incident.incidents
GROUP BY dimension, severity
ORDER BY dimension, severity;

-- KPI 4: среднее время разрешения
SELECT
    AVG(EXTRACT(EPOCH FROM (resolved_at - opened_at))/3600.0) AS avg_resolution_hours
FROM dq_incident.incidents
WHERE resolved_at IS NOT NULL;

-- KPI 5: quarantine
SELECT COUNT(*) AS quarantined_records
FROM dq_incident.quarantine_transactions;
