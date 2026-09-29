-- 1. Последние DQ-результаты
SELECT run_date, rule_code, dimension, status, metric_value, threshold_value, details
FROM dq.check_results
ORDER BY checked_at DESC;

-- 2. Качество по измерениям
SELECT dimension, status, COUNT(*) AS checks
FROM dq.check_results
GROUP BY dimension, status
ORDER BY dimension, status;

-- 3. Нарушения HIGH
SELECT run_date, rule_code, rule_name, table_name, metric_value, details
FROM dq.check_results r
JOIN dq.rule_catalog c USING (rule_code)
WHERE r.status = 'FAIL'
  AND c.criticality = 'HIGH'
ORDER BY checked_at DESC;

-- 4. Распределение транзакций по источникам
SELECT source_bank, COUNT(*) AS tx_count, SUM(amount) AS amount_total
FROM mart.fact_transactions
GROUP BY source_bank
ORDER BY source_bank;
