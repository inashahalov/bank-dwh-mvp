-- Демо «динамика метрик ведения метаданных»: три снимка за три дня.
-- Запуск (COMMENT ON требует владельца таблиц — их создал init-скрипт под airflow):
--   docker compose exec -T postgres psql -U airflow -d mart -v ON_ERROR_STOP=1 < data_governance/metadata_quality_demo.sql
-- Снимки датируются задним числом (сегодня −2, −1, сегодня), чтобы показать динамику за один запуск.
-- Скрипт можно перезапускать: сначала он убирает то, что ставит сам, снимки за эти три даты перезаписываются.

-- Сброс демо-описаний и демо-владельцев
COMMENT ON TABLE mart.dim_client            IS NULL;
COMMENT ON TABLE mart.dim_account           IS NULL;
COMMENT ON TABLE mart.fact_transactions     IS NULL;
COMMENT ON TABLE restricted.dim_client_pii  IS NULL;
COMMENT ON COLUMN mart.dim_client.full_name_hash         IS NULL;
COMMENT ON COLUMN mart.dim_client.initials               IS NULL;
COMMENT ON COLUMN mart.dim_client.age_band               IS NULL;
COMMENT ON COLUMN mart.dim_client.segment                IS NULL;
COMMENT ON COLUMN mart.dim_client.kyc_risk_level         IS NULL;
COMMENT ON COLUMN mart.dim_client.source_bank            IS NULL;
COMMENT ON COLUMN mart.dim_account.product_type          IS NULL;
COMMENT ON COLUMN mart.fact_transactions.tx_type         IS NULL;
COMMENT ON COLUMN mart.fact_transactions.is_flagged      IS NULL;
COMMENT ON COLUMN mart.fact_transactions.load_batch_date IS NULL;
DELETE FROM governance.object_owner
WHERE object_fqn IN ('mart.dim_client', 'mart.dim_account', 'mart.fact_transactions', 'restricted.dim_client_pii');

-- День 1. Исходное состояние: описаний и владельцев нет
SELECT governance.take_metadata_snapshot(current_date - 2);

-- День 2. Описания: переносим в COMMENT ON то, что уже написано комментариями в mart/init.sql
COMMENT ON TABLE mart.dim_client            IS 'Измерение: клиент. Только псевдонимизированные атрибуты; ФИО и дата рождения — в restricted.dim_client_pii';
COMMENT ON TABLE mart.dim_account           IS 'Измерение: счёт/продукт. Пересоздаётся целиком каждый день';
COMMENT ON TABLE mart.fact_transactions     IS 'Факт: транзакции. Инкрементальная загрузка по load_batch_date';
COMMENT ON TABLE restricted.dim_client_pii  IS 'ПДн клиента: ФИО и дата рождения. Доступ только у data_steward';

COMMENT ON COLUMN mart.dim_client.full_name_hash     IS 'md5(ФИО) — только для join и сверки, имя не восстановить';
COMMENT ON COLUMN mart.dim_client.initials           IS 'Инициалы для читаемости отчётов';
COMMENT ON COLUMN mart.dim_client.age_band           IS 'Возрастная группа вместо даты рождения: 18-24 / 25-34 / ... / 65+';
COMMENT ON COLUMN mart.dim_client.segment            IS 'Сегмент клиента: retail / premium / private';
COMMENT ON COLUMN mart.dim_client.kyc_risk_level     IS 'Уровень KYC-риска: low / medium / high (антифрод-атрибут)';
COMMENT ON COLUMN mart.dim_client.source_bank        IS 'Симулируемый банк-источник: vtb / sber / rshb';
COMMENT ON COLUMN mart.dim_account.product_type      IS 'Тип продукта: card / deposit / loan / current';
COMMENT ON COLUMN mart.fact_transactions.tx_type     IS 'Тип операции: purchase / transfer_in / transfer_out / withdrawal / fee';
COMMENT ON COLUMN mart.fact_transactions.is_flagged  IS 'Простой фрод-флаг';
COMMENT ON COLUMN mart.fact_transactions.load_batch_date IS 'Дата загрузочного батча (watermark инкрементальной загрузки)';

SELECT governance.take_metadata_snapshot(current_date - 1);

-- День 3. Владельцы и стюарды. Значения демо — в проекте их назначает Политика.
-- У таблицы ПДн бизнес-владелец намеренно не указан: это и есть находка для дашборда.
INSERT INTO governance.object_owner (object_fqn, business_owner, data_steward) VALUES
('mart.dim_client',           'Розничный бизнес (демо)', 'Data Steward'),
('mart.dim_account',          'Розничный бизнес (демо)', 'Data Steward'),
('mart.fact_transactions',    'Розничный бизнес (демо)', 'Data Steward'),
('restricted.dim_client_pii', NULL,                      'Data Steward')
ON CONFLICT (object_fqn) DO UPDATE
   SET business_owner = EXCLUDED.business_owner,
       data_steward   = EXCLUDED.data_steward,
       updated_at     = CURRENT_TIMESTAMP;

SELECT governance.take_metadata_snapshot(current_date);

-- Результат
SELECT metric_code, metric_name, numerator, denominator, value_pct, target_pct, meets_target
FROM governance.v_metadata_quality;

SELECT snapshot_date, metric_code, value_pct
FROM governance.metadata_quality_snapshot
ORDER BY snapshot_date, metric_code;
