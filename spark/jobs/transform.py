"""
Spark-джоб трансформации raw -> mart.

Вызывается через spark-submit с одним позиционным аргументом: дата батча (ds, YYYY-MM-DD).

Шаги:
  1. читает raw/clients, raw/accounts за все банки (полный снепшот на дату ds)
  2. читает raw/transactions за ds по всем банкам
  3. приводит типы, считает производные поля
  4. ФИО и точная дата рождения уходят ТОЛЬКО в restricted.dim_client_pii;
     в mart.dim_client попадают исключительно псевдонимизированные атрибуты
     (full_name_hash, initials, age_band) — см. data_governance/data_dictionary.md
  5. пишет dim_client, dim_account, restricted.dim_client_pii через mode="append".
     ВАЖНО: раньше здесь стоял mode="overwrite" — для Postgres Spark JDBC
     не умеет TRUNCATE при overwrite (isCascadingTruncateTable=true у
     Postgres-диалекта), поэтому overwrite = DROP TABLE + CREATE TABLE,
     что сносило PRIMARY KEY из init.sql после первого же прогона. Теперь
     Airflow перед этим шагом делает TRUNCATE TABLE (см. transform_load_mart_dag.py,
     таск truncate_dim_tables) — структура и PK сохраняются, Spark только
     наполняет уже существующую таблицу.
  6. пишет fact_transactions (append — идемпотентность инкремента за дату
     решена на уровне Airflow: delete_existing_batch перед этим шагом,
     см. transform_load_mart_dag.py)
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BANKS = ["vtb", "sber", "rshb"]
MART_JDBC_URL = "jdbc:postgresql://postgres:5432/mart"
MART_PROPS = {"user": "mart", "password": "mart", "driver": "org.postgresql.Driver"}
BUCKET = "datalake"


def read_raw_json(spark, entity: str, ds: str):
    paths = [f"s3a://{BUCKET}/raw/{entity}/bank={bank}/dt={ds}/{entity}.json" for bank in BANKS]
    return spark.read.option("multiline", "true").json(paths)


def dedup_with_reject(df, key: str, entity: str, ds: str):
    """Дедупликация по бизнес-ключу без молчаливой потери информации.

    Все строки, чей ключ встречается больше одного раза, сохраняются как есть
    в s3a://datalake/reject/<entity>/dt=<ds>/, количество пишется в лог.
    В mart уходит одна строка на ключ. Раньше здесь был голый dropDuplicates.
    """
    dup_rows = (
        df.withColumn("_key_cnt", F.count(F.lit(1)).over(Window.partitionBy(key)))
          .filter(F.col("_key_cnt") > 1)
          .drop("_key_cnt")
    )
    n_dup_rows = dup_rows.count()
    if n_dup_rows:
        n_dup_keys = dup_rows.select(key).distinct().count()
        dup_rows.write.mode("overwrite").json(f"s3a://{BUCKET}/reject/{entity}/dt={ds}")
        print(f"[{ds}] WARNING {entity}: {n_dup_keys} дублирующихся ключей "
              f"({n_dup_rows} строк) -> reject/{entity}/dt={ds}; в mart оставлена одна строка на ключ")
    else:
        print(f"[{ds}] {entity}: дублей по {key} нет")
    return df.dropDuplicates([key])


def age_band_expr(birth_date_col):
    age = F.floor(F.datediff(F.current_date(), birth_date_col) / 365.25)
    return (
        F.when(age < 25, "18-24")
         .when(age < 35, "25-34")
         .when(age < 45, "35-44")
         .when(age < 55, "45-54")
         .when(age < 65, "55-64")
         .otherwise("65+")
    )


def main(ds: str):
    spark = (
        SparkSession.builder.appName(f"dwh-mvp-transform-{ds}")
        .getOrCreate()
    )

    raw_clients_df = dedup_with_reject(
        read_raw_json(spark, "clients", ds).select(
            "client_id", "full_name",
            F.to_date("birth_date").alias("birth_date"),
            "segment", "region", "kyc_risk_level", "source_bank",
        ),
        "client_id", "clients", ds,
    )

    # --- реальные ПДн: только restricted, наружу не идут ---
    clients_pii_df = raw_clients_df.select(
        "client_id", "full_name", "birth_date", "source_bank",
    )

    # --- псевдонимизированное измерение для mart ---
    clients_df = raw_clients_df.select(
        "client_id",
        F.md5("full_name").alias("full_name_hash"),
        F.regexp_replace(F.col("full_name"), r"(\S)\S*\s*", "$1").alias("initials"),
        age_band_expr(F.col("birth_date")).alias("age_band"),
        "segment", "region", "kyc_risk_level", "source_bank",
    )

    accounts_df = dedup_with_reject(
        read_raw_json(spark, "accounts", ds).select(
            "account_id", "client_id", "product_type", "currency",
            F.to_date("opened_at").alias("opened_at"),
            "is_active", "source_bank",
        ),
        "account_id", "accounts", ds,
    )

    tx_df = dedup_with_reject(
        read_raw_json(spark, "transactions", ds).select(
            "transaction_id", "account_id", "client_id",
            F.to_timestamp("tx_ts").alias("tx_ts"),
            "tx_type", F.col("amount").cast("decimal(18,2)").alias("amount"),
            "currency", "merchant_category", "is_flagged", "source_bank",
        ).withColumn("load_batch_date", F.lit(ds).cast("date")),
        "transaction_id", "transactions", ds,
    )

    n_clients, n_accounts, n_tx = clients_df.count(), accounts_df.count(), tx_df.count()
    print(f"[{ds}] clients={n_clients} accounts={n_accounts} transactions={n_tx}")

    clients_pii_df.write.jdbc(MART_JDBC_URL, "restricted.dim_client_pii", mode="append", properties=MART_PROPS)
    clients_df.write.jdbc(MART_JDBC_URL, "mart.dim_client", mode="append", properties=MART_PROPS)
    accounts_df.write.jdbc(MART_JDBC_URL, "mart.dim_account", mode="append", properties=MART_PROPS)
    tx_df.write.jdbc(MART_JDBC_URL, "mart.fact_transactions", mode="append", properties=MART_PROPS)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: transform.py <ds:YYYY-MM-DD>")
    main(sys.argv[1])
