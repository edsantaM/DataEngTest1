from __future__ import annotations

from datetime import timedelta
from io import BytesIO
import logging
from pathlib import Path

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
TARGET_BUCKET = "bck-bronze"
TARGET_KEY = "master/data_prueba_tecnica.parquet"


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
        import tempfile

        import boto3
        import numpy as np
        import pandas as pd
        from airflow.hooks.base import BaseHook

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        logger.info("process_data: start")
        src_bucket = SOURCE_BUCKET
        src_path = SOURCE_KEY
        tgt_bucket = TARGET_BUCKET
        tgt_path = TARGET_KEY

        try:
            minio_conn = BaseHook.get_connection("minio_default")
            endpoint_url = minio_conn.extra_dejson.get(
                "endpoint_url",
                f"http://{minio_conn.host}:{minio_conn.port or 9000}",
            )
            access_key = minio_conn.login
            secret_key = minio_conn.password
        except Exception:
            logger.exception("Error retrieving MinIO connection")
            raise

        if not access_key or not secret_key:
            raise ValueError("The Airflow connection 'minio_default' must include login and password.")

        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=minio_conn.extra_dejson.get("region_name", "us-east-1"),
            )

            csv_bytes = s3.get_object(Bucket=src_bucket, Key=src_path)["Body"].read()

            raw = pd.read_csv(
                BytesIO(csv_bytes),
                dtype="string",
                keep_default_na=True,
                na_values=["", "null", "NULL"],
            )
        except Exception:
            logger.exception("Error reading CSV from S3")
            raise

        try:
            raw["amount"] = raw["amount"].astype("float64")
            raw["created_at"] = pd.to_datetime(raw["created_at"], errors="coerce").dt.date
            raw["paid_at"] = pd.to_datetime(raw["paid_at"], errors="coerce").dt.date
            stg = raw.copy()

            stg = stg[stg["id"].notna()]

            name_to_company = (
                raw.dropna(subset=["company_id", "name"])
                .groupby("name", as_index=False)["company_id"]
                .first()
                .rename(columns={"company_id": "canonical_company_id"})
            )

            company_to_name = (
                raw.dropna(subset=["company_id", "name"])
                .groupby("company_id", as_index=False)["name"]
                .first()
                .rename(columns={"name": "canonical_name"})
            )

            stg = stg.merge(name_to_company, on="name", how="left")
            stg = stg.merge(company_to_name, on="company_id", how="left")
            stg["company_id_filled"] = stg["company_id"].fillna(stg["canonical_company_id"])
            stg["name_filled"] = stg["name"].fillna(stg["canonical_name"])
            stg = stg.drop(columns=["canonical_company_id", "canonical_name"])

            valid_statuses = {
                "paid",
                "voided",
                "pending_payment",
                "pre_authorized",
                "refunded",
                "charged_back",
                "expired",
                "partially_refunded",
            }

            stg["status_clean"] = stg["status"].where(stg["status"].isin(valid_statuses), "unknown")

            stg.loc[stg["name"].isin(["MiP0xFFFF", "MiPas0xFFFF"]), "name_filled"] = "MiPasajefy"

            amount_num = stg["amount"].astype("float64")
            p99 = amount_num.quantile(0.99)

            mask_amounts = (
                amount_num.notna()
                & np.isfinite(amount_num)
                & amount_num.ge(0)
                & amount_num.le(p99)
            )

            stg = stg[mask_amounts]

            stg = stg.drop(columns=["company_id", "name", "status"])
            stg = stg.rename(
                columns={
                    "company_id_filled": "company_id",
                    "name_filled": "name",
                    "status_clean": "status",
                }
            )

            stg["is_paid"] = stg["status"].eq("paid")
            stg["pendig_payment"] = stg["status"].eq("pending_payment")
            stg["paid_amount"] = stg["amount"].where(stg["is_paid"], 0.0)
            stg["pending_amount"] = stg["amount"].where(stg["pendig_payment"], 0.0)
        except Exception:
            logger.exception("Error during data cleaning")
            raise

        try:
            aggregated = (
                stg.groupby(["name", "created_at"], as_index=False)
                .agg(
                    transactions=("id", "size"),
                    total_amount=("amount", "sum"),
                    average_amount=("amount", "mean"),
                    paid_transactions=("is_paid", "sum"),
                    paid_amount=("paid_amount", "sum"),
                    pending_payment_transactions=("pendig_payment", "sum"),
                    pending_amount=("pending_amount", "sum"),
                    min_amount=("amount", "min"),
                    max_amount=("amount", "max"),
                )
                .sort_values(["created_at", "name"])
            )
        except Exception:
            logger.exception("Error during data aggregation")
            raise

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                parquet_path = Path(tmpdir) / "data_prueba_tecnica.parquet"
                aggregated.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
                s3.upload_file(str(parquet_path), tgt_bucket, tgt_path)
        except Exception:
            logger.exception("Error writing Parquet to S3")
            raise

        return {
            "source_bucket": src_bucket,
            "source_key": src_path,
            "target_bucket": tgt_bucket,
            "target_key": tgt_path,
            "rows_written": int(aggregated.shape[0]),
        }

    @task
    
    def load_to_trino(load_result: dict):
        from trino.dbapi import connect
        import logging
        from airflow.hooks.base import BaseHook
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        #variables 
        schema_location = "s3a://bck-bronze/prueba"
        table_location = "s3a://bck-bronze/master/"

        try:
            trino_conn = BaseHook.get_connection("trino_default")
            trino_host = trino_conn.host
            trino_port = trino_conn.port
            trino_user = trino_conn.login
            extra = trino_conn.extra_dejson  
        except Exception:
            logger.exception("Error retrieving Trino connection")
            raise
        
        try:
            conn = connect(
                host=trino_host,
                port=trino_port,
                user=trino_user,
                catalog="bronze",
                schema="prueba",
                http_scheme="http",
            )

            cursor = conn.cursor()
        except Exception:
            logger.exception("Error connecting to Trino")
            cursor.close()
            conn.close()
            raise

        
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS bronze.prueba with (LOCATION = '{schema_location}')"

        create_table_query = f"""
                CREATE TABLE IF NOT EXISTS bronze.prueba.tbl_data (
                    name varchar,
                    created_at date,
                    transactions bigint,
                    total_amount double,
                    average_amount double,
                    paid_transactions bigint,
                    paid_amount double,
                    pending_payment_transactions  bigint,
                    pending_amount double,
                    min_amount double,
                    max_amount double
                )
                WITH (
                    external_location = '{table_location}',
                    format = 'PARQUET'
                )
                """
        try:
            cursor.execute(create_schema_query)
            cursor.execute(create_table_query)

            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error creating schema or table: {e}")
            cursor.close()
            conn.close()
            raise

    load_result = process_data()
    load_to_trino(load_result)


dag = etl_engineer_challenge()
