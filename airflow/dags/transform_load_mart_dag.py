"""
DAG 2: transform_load_mart
Ждёт завершения extract_raw за ту же дату -> Spark читает raw-слой из MinIO
(s3a://datalake/raw/...), строит dim/fact и пишет их напрямую в Postgres-mart
через JDBC.

Пакеты Spark (hadoop-aws, postgresql jdbc) подтягиваются через --packages
при spark-submit — в реальном проекте их стоит зашить в образ, чтобы не
зависеть от Maven Central на каждый прогон (см. README, "Что оптимизировать").
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "dwh-mvp",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.3",
])

with DAG(
    dag_id="transform_load_mart",
    description="Spark: raw (MinIO) -> dim/fact (Postgres mart)",
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dwh-mvp", "transform"],
) as dag:

    wait_for_extract = ExternalTaskSensor(
        task_id="wait_for_extract_raw",
        external_dag_id="extract_raw",
        external_task_id="extract_transactions",
        allowed_states=["success"],
        timeout=1800,
        poke_interval=30,
        mode="reschedule",
    )

    # обеспечивает идемпотентность: бэкфилл/ретрай за ту же дату не даст
    # дублей и не упадёт на PK транзакции (Spark потом делает append)
    delete_batch = PostgresOperator(
        task_id="delete_existing_batch",
        postgres_conn_id="mart_postgres",
        sql="DELETE FROM mart.fact_transactions WHERE load_batch_date = '{{ ds }}';",
    )

    # dim_client/dim_account/restricted.dim_client_pii — полный дневной снепшот.
    # TRUNCATE (а не Spark mode="overwrite") — принципиально: overwrite для
    # Postgres через Spark JDBC всегда делает DROP+CREATE и сносит PRIMARY KEY,
    # заведённый в init.sql. TRUNCATE сохраняет структуру таблицы, Spark дальше
    # просто добавляет (append) свежие строки в уже существующую таблицу.
    truncate_dims = PostgresOperator(
        task_id="truncate_dim_tables",
        postgres_conn_id="mart_postgres",
        sql="TRUNCATE TABLE mart.dim_client, mart.dim_account, restricted.dim_client_pii;",
    )

    spark_transform = SparkSubmitOperator(
        task_id="spark_transform_to_mart",
        application="/opt/spark/jobs/transform.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        application_args=["{{ ds }}"],
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            # client-mode: driver живёт в контейнере airflow-scheduler (LocalExecutor),
            # executor'ы на spark-worker должны уметь достучаться до него по имени сервиса
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.executor.memory": "768m",
        },
        verbose=True,
    )

    wait_for_extract >> [delete_batch, truncate_dims] >> spark_transform
