-- ============================================================
-- METADATA QUALITY
-- Метрики качества ведения метаданных + DQ с привязкой к объектам каталога
-- + снимки по дням для динамики. Накатывается после mart/init.sql.
-- Идемпотентен: повторный прогон ничего не ломает.
--
-- Описание объекта = COMMENT ON (то, что забирает сканер каталога).
-- Бизнес-владелец и стюард — атрибуты каталога, в СУБД их нет
-- (relowner — это роль СУБД, а не владелец данных), поэтому отдельная таблица.
-- ============================================================

\connect mart

-- 1. Справочник метрик. target_pct — целевое значение; в проекте его задаёт Политика.
CREATE TABLE IF NOT EXISTS governance.metadata_metric (
    metric_code   TEXT PRIMARY KEY,
    metric_name   TEXT NOT NULL,
    object_level  TEXT NOT NULL CHECK (object_level IN ('table', 'column')),
    target_pct    NUMERIC(5,1) NOT NULL
);

INSERT INTO governance.metadata_metric (metric_code, metric_name, object_level, target_pct) VALUES
('META-01', 'Таблицы с описанием',          'table',  100),
('META-02', 'Колонки с описанием',          'column',  90),
('META-03', 'Таблицы с бизнес-владельцем',  'table',  100),
('META-04', 'Таблицы со стюардом',          'table',  100),
('META-05', 'CDE, покрытые DQ-правилом',    'column', 100)
ON CONFLICT (metric_code) DO NOTHING;

-- 2. Атрибуты каталога уровня таблицы
CREATE TABLE IF NOT EXISTS governance.object_owner (
    object_fqn      TEXT PRIMARY KEY,        -- 'mart.fact_transactions'
    business_owner  TEXT,
    data_steward    TEXT,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Объекты каталога: таблицы и колонки периметра + атрибуты + число активных DQ-правил.
--    CDE/PII берутся из S2T, правила — из dq.rule_catalog.object_name ('schema.table.column').
CREATE OR REPLACE VIEW governance.v_catalog_object AS
WITH rel AS (
    SELECT c.oid, n.nspname::text AS schema_name, c.relname::text AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND NOT c.relispartition                     -- секции не считаем отдельными объектами
      AND n.nspname IN ('mart', 'restricted')      -- периметр каталога
),
flags AS (
    SELECT target_object || '.' || target_column AS object_fqn,
           bool_or(cde) AS is_cde,
           bool_or(pii) AS is_pii
    FROM governance.s2t_mapping
    GROUP BY 1
),
rules AS (
    SELECT object_name, count(*)::int AS dq_rules
    FROM dq.rule_catalog
    WHERE active
    GROUP BY 1
)
SELECT r.schema_name || '.' || r.table_name                     AS object_fqn,
       'table'::text                                            AS object_level,
       r.schema_name,
       r.table_name,
       NULL::text                                               AS column_name,
       NULLIF(btrim(obj_description(r.oid, 'pg_class')), '')    AS description,
       NULLIF(btrim(o.business_owner), '')                      AS business_owner,
       NULLIF(btrim(o.data_steward), '')                        AS data_steward,
       NULL::boolean                                            AS is_cde,
       NULL::boolean                                            AS is_pii,
       (SELECT coalesce(sum(q.dq_rules), 0)::int
          FROM rules q
         WHERE q.object_name = r.schema_name || '.' || r.table_name
            OR starts_with(q.object_name, r.schema_name || '.' || r.table_name || '.')) AS dq_rules
FROM rel r
LEFT JOIN governance.object_owner o ON o.object_fqn = r.schema_name || '.' || r.table_name
UNION ALL
SELECT r.schema_name || '.' || r.table_name || '.' || a.attname,
       'column',
       r.schema_name,
       r.table_name,
       a.attname::text,
       NULLIF(btrim(col_description(r.oid, a.attnum)), ''),
       NULL,
       NULL,
       coalesce(f.is_cde, false),
       coalesce(f.is_pii, false),
       coalesce(q.dq_rules, 0)
FROM rel r
JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN flags f ON f.object_fqn  = r.schema_name || '.' || r.table_name || '.' || a.attname
LEFT JOIN rules q ON q.object_name = r.schema_name || '.' || r.table_name || '.' || a.attname;

-- 4. Текущие значения метрик ведения метаданных
CREATE OR REPLACE VIEW governance.v_metadata_quality AS
WITH o AS (
    SELECT * FROM governance.v_catalog_object
),
m AS (
    SELECT 'META-01' AS metric_code,
           count(*) FILTER (WHERE description IS NOT NULL) AS numerator,
           count(*) AS denominator
    FROM o WHERE object_level = 'table'
    UNION ALL
    SELECT 'META-02', count(*) FILTER (WHERE description IS NOT NULL), count(*)
    FROM o WHERE object_level = 'column'
    UNION ALL
    SELECT 'META-03', count(*) FILTER (WHERE business_owner IS NOT NULL), count(*)
    FROM o WHERE object_level = 'table'
    UNION ALL
    SELECT 'META-04', count(*) FILTER (WHERE data_steward IS NOT NULL), count(*)
    FROM o WHERE object_level = 'table'
    UNION ALL
    SELECT 'META-05', count(*) FILTER (WHERE dq_rules > 0), count(*)
    FROM o WHERE is_cde
)
SELECT d.metric_code,
       d.metric_name,
       m.numerator::int                                              AS numerator,
       m.denominator::int                                            AS denominator,
       round(100.0 * m.numerator / NULLIF(m.denominator, 0), 1)      AS value_pct,
       d.target_pct,
       round(100.0 * m.numerator / NULLIF(m.denominator, 0), 1) >= d.target_pct AS meets_target
FROM m
JOIN governance.metadata_metric d USING (metric_code)
ORDER BY d.metric_code;

-- 5. Снимки по дням — для графика динамики. Повторный вызов за ту же дату перезаписывает снимок.
CREATE TABLE IF NOT EXISTS governance.metadata_quality_snapshot (
    snapshot_date  DATE NOT NULL,
    metric_code    TEXT NOT NULL REFERENCES governance.metadata_metric (metric_code),
    numerator      INT  NOT NULL,
    denominator    INT  NOT NULL,
    value_pct      NUMERIC(5,1),
    taken_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_date, metric_code)
);

CREATE OR REPLACE FUNCTION governance.take_metadata_snapshot(p_date DATE DEFAULT CURRENT_DATE)
RETURNS INT
LANGUAGE sql
AS $$
    WITH ins AS (
        INSERT INTO governance.metadata_quality_snapshot
               (snapshot_date, metric_code, numerator, denominator, value_pct)
        SELECT p_date, metric_code, numerator, denominator, value_pct
        FROM governance.v_metadata_quality
        ON CONFLICT (snapshot_date, metric_code) DO UPDATE
           SET numerator   = EXCLUDED.numerator,
               denominator = EXCLUDED.denominator,
               value_pct   = EXCLUDED.value_pct,
               taken_at    = CURRENT_TIMESTAMP
        RETURNING 1
    )
    SELECT count(*)::int FROM ins;
$$;

-- 6. Quality rate по дням: последний результат каждого правила за дату
--    (check_results — журнал, перезапуски DQ дописывают строки; логика та же, что в Streamlit)
CREATE OR REPLACE VIEW dq.v_daily_quality AS
SELECT run_date,
       count(*)::int                                                         AS checks,
       count(*) FILTER (WHERE status = 'PASS')::int                          AS passed,
       round(100.0 * count(*) FILTER (WHERE status = 'PASS') / count(*), 1)  AS quality_pct
FROM (
    SELECT DISTINCT ON (run_date, rule_code) run_date, rule_code, status
    FROM dq.check_results
    ORDER BY run_date, rule_code, checked_at DESC, check_id DESC
) last_result
GROUP BY run_date;

-- 7. Качество данных с привязкой к объектам каталога (последний прогон)
CREATE OR REPLACE VIEW governance.v_object_quality AS
WITH last_result AS (
    SELECT DISTINCT ON (run_date, rule_code) run_date, rule_code, status
    FROM dq.check_results
    ORDER BY run_date, rule_code, checked_at DESC, check_id DESC
),
latest AS (
    SELECT * FROM last_result
    WHERE run_date = (SELECT max(run_date) FROM dq.check_results)
),
rule_obj AS (
    SELECT split_part(r.object_name, '.', 1) || '.' || split_part(r.object_name, '.', 2) AS table_fqn,
           r.rule_code, r.criticality, l.status, l.run_date
    FROM dq.rule_catalog r
    LEFT JOIN latest l USING (rule_code)
    WHERE r.active
)
SELECT c.object_fqn                                                     AS table_fqn,
       c.business_owner,
       c.data_steward,
       c.description IS NOT NULL                                        AS has_description,
       count(ro.rule_code)::int                                         AS dq_rules,
       count(*) FILTER (WHERE ro.status = 'FAIL')::int                  AS dq_failed,
       count(*) FILTER (WHERE ro.status = 'FAIL' AND ro.criticality = 'HIGH')::int AS dq_failed_high,
       max(ro.run_date)                                                 AS last_run_date,
       round(100.0 * count(*) FILTER (WHERE ro.status = 'PASS')
             / NULLIF(count(ro.status), 0), 1)                          AS dq_quality_pct
FROM governance.v_catalog_object c
LEFT JOIN rule_obj ro ON ro.table_fqn = c.object_fqn
WHERE c.object_level = 'table'
GROUP BY c.object_fqn, c.business_owner, c.data_steward, c.description;

-- 8. Рабочий список стюарда: что дозаполнить (таблицы, затем CDE, затем остальное)
CREATE OR REPLACE VIEW governance.v_metadata_gaps AS
SELECT object_fqn, object_level, is_cde, gaps
FROM (
    SELECT object_fqn, object_level, is_cde,
           array_to_string(array_remove(ARRAY[
               CASE WHEN description IS NULL THEN 'нет описания' END,
               CASE WHEN object_level = 'table' AND business_owner IS NULL THEN 'нет владельца' END,
               CASE WHEN object_level = 'table' AND data_steward IS NULL THEN 'нет стюарда' END,
               CASE WHEN is_cde AND dq_rules = 0 THEN 'CDE без DQ-правила' END
           ], NULL), ', ') AS gaps
    FROM governance.v_catalog_object
) g
WHERE gaps <> ''
ORDER BY (object_level = 'table') DESC, is_cde DESC, object_fqn;

-- 9. Права. Объекты создаёт суперпользователь init-скрипта — выдаём явно (как в init.sql).
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA governance TO mart;
GRANT SELECT ON dq.v_daily_quality TO mart;

-- Отдельная read-only роль для Grafana: дашборд не ходит в БД под ETL-ролью
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE mart TO grafana_ro;
GRANT USAGE ON SCHEMA dq, dq_incident, governance TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA dq, dq_incident, governance TO grafana_ro;
