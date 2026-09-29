-- Выполняется один раз при первом старте контейнера postgres
-- (основная БД airflow создаётся образом автоматически из POSTGRES_DB)

CREATE USER mart WITH PASSWORD 'mart';
CREATE DATABASE mart OWNER mart;

\connect mart

CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION mart;

-- Измерение: клиент.
-- ФИО и точная дата рождения сюда НЕ попадают — это ПДн, они живут только
-- в restricted.dim_client_pii с ограниченным доступом (см. ниже).
-- mart.dim_client содержит только псевдонимизированные атрибуты:
--   full_name_hash — md5(ФИО), нужен исключительно для join/сверки, имя не восстановить
--   initials       — инициалы для читаемости отчётов ("И.П.")
--   age_band       — возрастная группа вместо точной даты рождения (data minimization)
CREATE TABLE mart.dim_client (
    client_id        BIGINT PRIMARY KEY,
    full_name_hash   TEXT,
    initials         TEXT,
    age_band         TEXT,        -- 18-24 / 25-34 / ... / 65+
    segment          TEXT,        -- retail / premium / private
    region           TEXT,
    kyc_risk_level   TEXT,        -- low / medium / high (антифрод-атрибут)
    source_bank      TEXT         -- какой банк симулируем: vtb / sber / rshb
);

-- Измерение: счёт/продукт
-- client_id намеренно без FK: dim_client — полный дневной снепшот
-- (TRUNCATE + Spark append, см. transform_load_mart_dag.py), а не
-- инкрементальная таблица. FK здесь не даёт ничего, кроме риска поймать
-- constraint violation на порядке truncate внутри одной транзакции —
-- в Postgres не выживет. Стандартная практика для DWH-измерений:
-- целостность проверяется на этапе DQ-проверок (DQ-005..DQ-007), а не FK в БД.
CREATE TABLE mart.dim_account (
    account_id       BIGINT PRIMARY KEY,
    client_id        BIGINT,
    product_type     TEXT,        -- card / deposit / loan / current
    currency         TEXT,
    opened_at        DATE,
    is_active        BOOLEAN,
    source_bank      TEXT
);

-- Факт: транзакции
CREATE TABLE mart.fact_transactions (
    transaction_id   BIGINT PRIMARY KEY,
    account_id       BIGINT,
    client_id        BIGINT,
    tx_ts            TIMESTAMP NOT NULL,
    tx_type          TEXT,        -- purchase / transfer_in / transfer_out / withdrawal / fee
    amount           NUMERIC(18,2),
    currency         TEXT,
    merchant_category TEXT,
    is_flagged       BOOLEAN DEFAULT FALSE,  -- простой фрод-флаг
    source_bank      TEXT,
    load_batch_date  DATE NOT NULL           -- watermark инкрементальной загрузки
);

CREATE INDEX idx_fact_tx_date ON mart.fact_transactions (tx_ts);
CREATE INDEX idx_fact_tx_batch ON mart.fact_transactions (load_batch_date);
CREATE INDEX idx_fact_tx_account ON mart.fact_transactions (account_id);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA mart TO mart;
GRANT ALL PRIVILEGES ON SCHEMA mart TO mart;


-- ============================================================
-- ПДн И РАЗГРАНИЧЕНИЕ ДОСТУПА (Data Governance)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS restricted AUTHORIZATION mart;

-- Единственное место во всей БД, где лежит реальное ФИО и дата рождения.
CREATE TABLE restricted.dim_client_pii (
    client_id     BIGINT PRIMARY KEY,
    full_name     TEXT NOT NULL,
    birth_date    DATE,
    source_bank   TEXT
);

-- analyst_ro   — типовой BI-потребитель: видит только маскированную mart
-- data_steward — расследует DQ-инциденты, поэтому имеет доступ к restricted
CREATE ROLE analyst_ro LOGIN PASSWORD 'analyst_ro';
CREATE ROLE data_steward LOGIN PASSWORD 'data_steward';

GRANT CONNECT ON DATABASE mart TO analyst_ro, data_steward;

REVOKE ALL ON SCHEMA restricted FROM PUBLIC;
GRANT USAGE ON SCHEMA restricted TO data_steward;
GRANT SELECT ON ALL TABLES IN SCHEMA restricted TO data_steward;
ALTER DEFAULT PRIVILEGES IN SCHEMA restricted GRANT SELECT ON TABLES TO data_steward;

GRANT USAGE ON SCHEMA mart TO analyst_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO analyst_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO analyst_ro;

-- Проверка на демо/собеседовании:
--   psql -h localhost -U analyst_ro -d mart -c "select * from restricted.dim_client_pii"
--   -> "permission denied for schema restricted"
--   psql -h localhost -U data_steward -d mart -c "select * from restricted.dim_client_pii"
--   -> отдаёт строки


-- ============================================================
-- DATA QUALITY / DATA GOVERNANCE
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dq AUTHORIZATION mart;

-- Журнал результатов DQ-проверок.
-- Каждая проверка имеет понятное имя, измерение качества,
-- фактическое значение, порог и статус PASS/FAIL.
CREATE TABLE dq.check_results (
    check_id        BIGSERIAL PRIMARY KEY,
    run_date        DATE NOT NULL,
    checked_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rule_code       TEXT NOT NULL,
    rule_name       TEXT NOT NULL,
    dimension       TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    metric_value    NUMERIC,
    threshold_value NUMERIC,
    status           TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    details         TEXT
);

CREATE INDEX idx_dq_results_run_date ON dq.check_results(run_date);
CREATE INDEX idx_dq_results_status ON dq.check_results(status);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dq TO mart;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dq TO mart;

-- Реестр DQ-правил: минимальный пример Data Governance metadata layer.
-- criticality используется в коде DAG (data_quality_dag.py) для решения,
-- останавливать ли pipeline — не только для документации.
CREATE TABLE dq.rule_catalog (
    rule_code       TEXT PRIMARY KEY,
    rule_name       TEXT NOT NULL,
    dimension       TEXT NOT NULL,
    criticality     TEXT NOT NULL CHECK (criticality IN ('HIGH','MEDIUM','LOW')),
    object_name     TEXT NOT NULL,
    description     TEXT NOT NULL,
    owner_role      TEXT NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO dq.rule_catalog
(rule_code, rule_name, dimension, criticality, object_name, description, owner_role)
VALUES
('DQ-001','Client ID is not null','completeness','HIGH','mart.dim_client.client_id',
 'Идентификатор клиента обязателен','Data Steward'),
('DQ-002','Transaction ID uniqueness','uniqueness','HIGH','mart.fact_transactions.transaction_id',
 'Идентификатор транзакции уникален','Data Steward'),
('DQ-003','Transaction amount is non-negative','validity','HIGH','mart.fact_transactions.amount',
 'Сумма транзакции не должна быть отрицательной','Data Steward'),
('DQ-004','Allowed transaction type','validity','MEDIUM','mart.fact_transactions.tx_type',
 'Тип транзакции должен входить в согласованный домен значений','Data Steward'),
('DQ-005','Transaction account exists','consistency','HIGH','mart.fact_transactions.account_id',
 'Каждая транзакция должна ссылаться на существующий счёт','Data Steward'),
('DQ-006','Transaction client exists','consistency','HIGH','mart.fact_transactions.client_id',
 'Каждая транзакция должна ссылаться на существующего клиента','Data Steward'),
('DQ-007','Account client exists','consistency','HIGH','mart.dim_account.client_id',
 'Каждый счёт должен быть связан с существующим клиентом','Data Steward'),
('DQ-008','Batch date is present','completeness','HIGH','mart.fact_transactions.load_batch_date',
 'Для каждой транзакции должна быть дата загрузки','Data Engineer'),
('DQ-009','Transaction timestamp is present','completeness','HIGH','mart.fact_transactions.tx_ts',
 'Время транзакции обязательно','Data Steward'),
('DQ-010','Daily batch has data','freshness','HIGH','mart.fact_transactions.load_batch_date',
 'За проверяемую дату должен присутствовать загруженный батч','Data Engineer')
ON CONFLICT (rule_code) DO NOTHING;

-- ============================================================
-- SOURCE-TO-TARGET MAPPING / DATA LINEAGE METADATA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS governance AUTHORIZATION mart;

CREATE TABLE governance.s2t_mapping (
    mapping_id      BIGSERIAL PRIMARY KEY,
    source_system   TEXT NOT NULL,
    source_object   TEXT NOT NULL,
    source_column   TEXT NOT NULL,
    target_object   TEXT NOT NULL,
    target_column   TEXT NOT NULL,
    transformation  TEXT NOT NULL,
    business_rule   TEXT,
    dq_rule_code    TEXT,
    cde             BOOLEAN NOT NULL DEFAULT FALSE,
    pii             BOOLEAN NOT NULL DEFAULT FALSE,
    owner_role      TEXT NOT NULL DEFAULT 'Data Steward'
);

CREATE TABLE governance.data_lineage (
    lineage_id      BIGSERIAL PRIMARY KEY,
    source_system   TEXT NOT NULL,
    source_layer    TEXT NOT NULL,
    source_object   TEXT NOT NULL,
    process_name    TEXT NOT NULL,
    target_layer    TEXT NOT NULL,
    target_object   TEXT NOT NULL,
    relation_type   TEXT NOT NULL,
    description     TEXT NOT NULL
);

INSERT INTO governance.s2t_mapping
(source_system, source_object, source_column, target_object, target_column,
 transformation, business_rule, dq_rule_code, cde, pii)
VALUES
('bank_api','clients','client_id','mart.dim_client','client_id','CAST -> BIGINT','Уникальный ID клиента','DQ-001',TRUE,FALSE),
('bank_api','clients','full_name','restricted.dim_client_pii','full_name','Direct mapping (ПДн, доступ только data_steward)','ФИО клиента','',FALSE,TRUE),
('bank_api','clients','full_name','mart.dim_client','full_name_hash','md5(full_name)','Псевдонимизация для join без раскрытия ФИО','',FALSE,TRUE),
('bank_api','clients','full_name','mart.dim_client','initials','regexp: первые буквы слов','Инициалы для читаемости отчётов','',FALSE,TRUE),
('bank_api','clients','birth_date','restricted.dim_client_pii','birth_date','CAST -> DATE (ПДн, доступ только data_steward)','Дата рождения','',FALSE,TRUE),
('bank_api','clients','birth_date','mart.dim_client','age_band','возрастная группа вместо точной даты','Data minimization — точный возраст в mart не нужен','',FALSE,TRUE),
('bank_api','clients','segment','mart.dim_client','segment','Direct mapping','Сегмент клиента','',FALSE,FALSE),
('bank_api','clients','region','mart.dim_client','region','Direct mapping','Регион клиента','',FALSE,FALSE),
('bank_api','clients','kyc_risk_level','mart.dim_client','kyc_risk_level','Direct mapping','Уровень KYC-риска','',TRUE,FALSE),
('bank_api','accounts','account_id','mart.dim_account','account_id','CAST -> BIGINT','Уникальный ID счёта','',TRUE,FALSE),
('bank_api','accounts','client_id','mart.dim_account','client_id','CAST -> BIGINT','Владелец счёта должен существовать','DQ-007',TRUE,FALSE),
('bank_api','accounts','product_type','mart.dim_account','product_type','Domain mapping','Тип банковского продукта','',FALSE,FALSE),
('bank_api','accounts','currency','mart.dim_account','currency','Direct mapping','Валюта счёта','',TRUE,FALSE),
('bank_api','accounts','opened_at','mart.dim_account','opened_at','CAST -> DATE','Дата открытия счёта','',FALSE,FALSE),
('bank_api','transactions','transaction_id','mart.fact_transactions','transaction_id','CAST -> BIGINT','Уникальный ID операции','DQ-002',TRUE,FALSE),
('bank_api','transactions','account_id','mart.fact_transactions','account_id','CAST -> BIGINT','Счёт операции','DQ-005',TRUE,FALSE),
('bank_api','transactions','client_id','mart.fact_transactions','client_id','CAST -> BIGINT','Клиент операции','DQ-006',TRUE,FALSE),
('bank_api','transactions','tx_ts','mart.fact_transactions','tx_ts','CAST -> TIMESTAMP','Время операции','DQ-009',TRUE,FALSE),
('bank_api','transactions','tx_type','mart.fact_transactions','tx_type','Domain validation','Тип операции из controlled vocabulary','DQ-004',FALSE,FALSE),
('bank_api','transactions','amount','mart.fact_transactions','amount','CAST -> DECIMAL(18,2)','Сумма неотрицательна','DQ-003',TRUE,FALSE),
('bank_api','transactions','currency','mart.fact_transactions','currency','Direct mapping','Валюта операции','',TRUE,FALSE),
('bank_api','transactions','source_bank','mart.fact_transactions','source_bank','Direct mapping','Источник данных','',TRUE,FALSE),
('airflow','batch_control','execution_date','mart.fact_transactions','load_batch_date','CAST -> DATE','Дата загрузочного батча (Airflow ds)','DQ-008',TRUE,FALSE)
ON CONFLICT DO NOTHING;

INSERT INTO governance.data_lineage
(source_system, source_layer, source_object, process_name, target_layer, target_object, relation_type, description)
VALUES
('bank_api','source','/clients','extract_raw -> MinIO','raw','raw/clients/bank=*/dt=*/clients.json','loads','Ежедневная выгрузка клиентов из API'),
('bank_api','source','/accounts','extract_raw -> MinIO','raw','raw/accounts/bank=*/dt=*/accounts.json','loads','Ежедневная выгрузка счетов из API'),
('bank_api','source','/transactions','extract_raw -> MinIO','raw','raw/transactions/bank=*/dt=*/transactions.json','loads','Ежедневная выгрузка транзакций из API'),
('MinIO','raw','raw/clients/*','Spark transform','restricted','restricted.dim_client_pii','transforms_to','Реальное ФИО/дата рождения — только сюда'),
('MinIO','raw','raw/clients/*','Spark transform','mart','mart.dim_client','transforms_to','Псевдонимизация (hash/инициалы/age_band) + deduplication по client_id'),
('MinIO','raw','raw/accounts/*','Spark transform','mart','mart.dim_account','transforms_to','Типизация и deduplication по account_id'),
('MinIO','raw','raw/transactions/*','Spark transform','mart','mart.fact_transactions','transforms_to','Типизация, watermark и deduplication по transaction_id'),
('mart','mart','mart.dim_client','data_quality_checks','dq','dq.check_results','validated_by','DQ-001 и связанные проверки'),
('mart','mart','mart.dim_account','data_quality_checks','dq','dq.check_results','validated_by','DQ-007'),
('mart','mart','mart.fact_transactions','data_quality_checks','dq','dq.check_results','validated_by','DQ-002..DQ-010'),
('dq','metadata','dq.rule_catalog','data_quality_checks','dq','dq.check_results','produces','Каталог правил определяет контролируемые объекты, критичность и ожидаемые значения'),
('dq','result','dq.check_results','auto_incident_on_fail','dq_incident','dq_incident.incidents','produces','Каждый FAIL автоматически заводит инцидент (см. data_quality_dag.py)'),
('dq','result','dq.check_results','streamlit_dq_dashboard','dashboard','Bank DQ Dashboard','consumed_by','Dashboard читает результаты DQ и отображает качество батча, историю и нарушения')
ON CONFLICT DO NOTHING;

-- Таблицы dq.rule_catalog и restricted.dim_client_pii создаются ПОСЛЕ своих первых
-- GRANT'ов и принадлежат суперпользователю init-скрипта, поэтому без этого блока
-- роль mart (Airflow/Spark/Dashboard) не может ни читать каталог правил, ни
-- делать TRUNCATE/INSERT в restricted.dim_client_pii.
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dq TO mart;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA restricted TO mart;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA governance TO mart;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA governance TO mart;


-- ============================================================
-- DQ INCIDENTS / QUARANTINE
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dq_incident AUTHORIZATION mart;

CREATE TABLE dq_incident.incidents (
    incident_id     BIGSERIAL PRIMARY KEY,
    opened_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_date        DATE,
    rule_code       TEXT NOT NULL,
    severity        TEXT NOT NULL CHECK (severity IN ('HIGH','MEDIUM','LOW')),
    dimension       TEXT NOT NULL,
    object_name     TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (
        status IN ('OPEN','TRIAGED','IN_REMEDIATION','RECHECK','RESOLVED','ACCEPTED')
    ),
    root_cause      TEXT,
    owner_role      TEXT,
    description     TEXT,
    resolved_at     TIMESTAMP,
    resolution      TEXT
);

CREATE TABLE dq_incident.quarantine_transactions (
    quarantine_id       BIGSERIAL PRIMARY KEY,
    incident_id         BIGINT NOT NULL REFERENCES dq_incident.incidents(incident_id),
    quarantined_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_id      BIGINT,
    account_id          BIGINT,
    client_id           BIGINT,
    tx_ts               TIMESTAMP,
    amount              NUMERIC,
    tx_type             TEXT,
    source_bank         TEXT,
    reason              TEXT NOT NULL
);

CREATE INDEX idx_dq_incident_status
    ON dq_incident.incidents(status);

CREATE INDEX idx_dq_quarantine_incident
    ON dq_incident.quarantine_transactions(incident_id);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dq_incident TO mart;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dq_incident TO mart;
