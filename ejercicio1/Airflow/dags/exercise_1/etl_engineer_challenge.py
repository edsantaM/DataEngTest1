from __future__ import annotations

from datetime import timedelta
from io import BytesIO

import pendulum
from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")
SOURCE_BUCKET = "bck-landing"
SOURCE_KEY = "data/data_prueba_tecnica.csv"


@dag(
    dag_id="etl_engineer_challenge",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline for landing to bronze and Trino publication",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    tags=["data-engineering", "challenge", "minio", "trino"],
)
def etl_engineer_challenge():
    @task
    def process_data():
        import logging

        import boto3
        import pandas as pd
        from airflow.hooks.base import BaseHook

        logger = logging.getLogger(__name__)

        minio_conn = BaseHook.get_connection("minio_default")
        endpoint_url = minio_conn.extra_dejson.get(
            "endpoint_url",
            f"http://{minio_conn.host}:{minio_conn.port or 9000}",
        )
        access_key = minio_conn.login
        secret_key = minio_conn.password

        if not access_key or not secret_key:
            raise ValueError("The Airflow connection 'minio_default' must include login and password.")

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=minio_conn.extra_dejson.get("region_name", "us-east-1"),
        )

        csv_bytes = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)["Body"].read()
        df = pd.read_csv(
            BytesIO(csv_bytes),
            dtype="string",
            keep_default_na=True,
            na_values=["", "null", "NULL"],
        )

        logger.info("CSV loaded successfully from MinIO")
        logger.info("DataFrame shape: rows=%s columns=%s", df.shape[0], df.shape[1])
        logger.info("Columns: %s", df.columns.tolist())

        return {
            "source_bucket": SOURCE_BUCKET,
            "source_key": SOURCE_KEY,
            "rows_loaded": int(df.shape[0]),
            "columns_loaded": df.columns.tolist(),
        }

    process_data()


dag = etl_engineer_challenge()
