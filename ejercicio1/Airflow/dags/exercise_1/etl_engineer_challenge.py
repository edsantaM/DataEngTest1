from __future__ import annotations

from datetime import timedelta
from io import BytesIO

from anyio import Path

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
        import os
        import boto3
        import pandas as pd
        from trino.dbapi import connect
        import logging
        from io import BytesIO
        import tempfile
        from airflow.hooks.base import BaseHook
        import numpy as np
        logging.basicConfig(level=logging.INFO)

        logger = logging.getLogger(__name__)
        #variables 
        src_bucket = "bck-landing"
        src_path = "data/data_prueba_tecnica.csv"
        tgt_bucket = "bck-bronze"
        tgt_path = "master/data_prueba_tecnica.parquet"

        minio_conn = BaseHook.get_connection("minio_default")
        endpoint_url = minio_conn.extra_dejson.get(
            "endpoint_url",
            f"http://{minio_conn.host}:{minio_conn.port or 9000}",
        )
        access_key = minio_conn.login
        secret_key = minio_conn.password

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

            csv_bytes = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)["Body"].read()
            raw = pd.read_csv(
                BytesIO(csv_bytes),
                dtype="string",
                keep_default_na=True,
                na_values=["", "null", "NULL"],
            )
        except Exception as e:
            logging.info(f"Error reading CSV from S3: {e}")
            raise
        #hasta aca solo se lee el csv, para el analisis se creó un notebook para la ejecucion mas práctica.

        #limpieza
        try:
            stg = raw.copy() 
            #1. omitir id nulos
            stg = stg[stg['id'].notna()]

            #2 y 3 mapear company_id a nombre y nombre a company id
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

            stg = stg.merge(name_to_company, on ="name", how="left")
            stg = stg.merge(company_to_name, on="company_id", how="left")
            stg["company_id_filled"] = stg["company_id"].fillna(stg["canonical_company_id"])
            stg["name_filled"] = stg["name"].fillna(stg["canonical_name"])
            stg = stg.drop(columns=[ "canonical_company_id", "canonical_name"])

            #4 columna de status
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

            #5. eliminar nombres sospechosos
            stg.loc[stg["name"].isin(["MiP0xFFFF", "MiPas0xFFFF"]), "name_filled"] = "MiPasajefy"

            #drop outliers en amount
            amount_num = pd.to_numeric(stg["amount"], errors="coerce")
            p99 = amount_num.quantile(0.99)

            mask_amounts = (
                amount_num.notna()
                & np.isfinite(amount_num)
                & amount_num.ge(0)
                & amount_num.le(p99)
            )

            stg_rejected = stg[~mask_amounts].copy()
            stg = stg[mask_amounts]


            #result 
            stg = stg.drop(columns=["company_id", "name", "status"])
            stg = stg.rename(columns={"company_id_filled": "company_id", "name_filled": "name", "status_clean": "status"})

            stg["is_paid"] = stg["status"].eq("paid")
            stg["pendig_payment"] = stg["status"].eq("pending_payment")
            stg["paid_amount"] = stg["amount"].where(stg["is_paid"], 0.0)
            stg["pending_amount"] = stg["amount"].where(stg["pendig_payment"], 0.0)
        except Exception as e:
            logging.info(f"Error during data cleaning: {e}")
            raise


        #agregaciones 
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
        except Exception as e:
            logging.info(f"Error during data aggregation: {e}")
            raise

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                parquet_path = Path(tmpdir) / "data_prueba_tecnica.parquet"
                aggregated.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
                s3.upload_file(str(parquet_path), tgt_bucket, tgt_path)
        except Exception as e:
            logging.info(f"Error writing Parquet to S3: {e}")
            raise

    process_data()


dag = etl_engineer_challenge()
