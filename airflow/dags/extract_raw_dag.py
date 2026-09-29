"""
DAG 1: extract_raw
Источник (API-симулятор трёх банков) -> raw-слой DataLake (MinIO, s3://datalake/raw/...)

Идемпотентно: перезапуск за ту же дату перезаписывает тот же ключ в MinIO
(симулятор детерминирован по дате), поэтому backfill безопасен.
"""
import io
import json
import os
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BANKS = ["vtb", "sber", "rshb"]
API_BASE_URL = os.environ.get("API_BASE_URL", "http://api-simulator:8000")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "datalake")


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _ensure_bucket(s3) -> None:
    """Бакет создаём здесь: образ minio/mc недоступен, отдельного minio-init больше нет.
    Два параллельных таска могут создавать одновременно — 'уже существует' не ошибка."""
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        try:
            s3.create_bucket(Bucket=MINIO_BUCKET)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise


def _put_json(s3, key: str, payload) -> None:
    buf = io.BytesIO(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    s3.upload_fileobj(buf, MINIO_BUCKET, key)


def extract_entity(entity: str, ds: str, **_):
    """entity: clients | accounts | transactions"""
    s3 = _s3_client()
    _ensure_bucket(s3)
    for bank in BANKS:
        params = {"bank": bank, "limit": 500}
        if entity == "transactions":
            params["date"] = ds
        resp = requests.get(f"{API_BASE_URL}/{entity}", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        key = f"raw/{entity}/bank={bank}/dt={ds}/{entity}.json"
        _put_json(s3, key, data)
        print(f"[{entity}/{bank}] {len(data)} записей -> s3://{MINIO_BUCKET}/{key}")


default_args = {
    "owner": "dwh-mvp",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="extract_raw",
    description="Симулятор API -> raw-слой DataLake (MinIO)",
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dwh-mvp", "extract"],
) as dag:

    extract_clients = PythonOperator(
        task_id="extract_clients",
        python_callable=extract_entity,
        op_kwargs={"entity": "clients", "ds": "{{ ds }}"},
    )

    extract_accounts = PythonOperator(
        task_id="extract_accounts",
        python_callable=extract_entity,
        op_kwargs={"entity": "accounts", "ds": "{{ ds }}"},
    )

    extract_transactions = PythonOperator(
        task_id="extract_transactions",
        python_callable=extract_entity,
        op_kwargs={"entity": "transactions", "ds": "{{ ds }}"},
    )

    [extract_clients, extract_accounts] >> extract_transactions
