from __future__ import annotations

from datetime import timedelta
from io import BytesIO
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

@dag(
    dag_id="station_status_pipe",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline for landing to bronze and Trino publication",
    start_date=START_DATE,
    schedule="*/10 * * * *",
    catchup=False,
    tags=["data-engineering", "challenge", "minio", "trino"],
)
def status_stations_pipe():
    @task
    def load_to_bronze():
        import pandas as pd
        import logging
        logging.basicConfig(level=logging.INFO)
        import tempfile
        from pathlib import Path
        import boto3

        from airflow.hooks.base import BaseHook

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        try:

            url = "https://gbfs.mex.lyftbikes.com/gbfs/es/station_status.json"

            raw = pd.read_json(url)
            last_updated = raw["last_updated"].iloc[0]
            stations = pd.json_normalize(raw["data"]["stations"])
            stations['last_updated']= last_updated

            bool_cols = ["is_installed", "is_renting", "is_returning","is_charging",'eightd_has_available_keys']

            for col in bool_cols:
                stations[col] = stations[col].astype("boolean")

            stations["last_updated_ts"] = pd.to_datetime(stations["last_updated"], unit="s", utc=True)
            stations["last_reported_ts"] = pd.to_datetime(stations["last_reported"], unit="s", utc=True)
            run_id = stations["last_updated_ts"].iloc[0].strftime("%Y%m%d%H%M")
            stations["run_id"] = run_id

            stations = stations.drop(columns=["last_updated", "last_reported"])

        except Exception as e:
            logging.error(f"Error en la extraccion de datos desde la fuente: {e}")
            raise

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

            tgt_bucket = "bck-bronze"
            tgt_key = f"station_status/data/run_id={run_id}/station_status.parquet"

            with tempfile.TemporaryDirectory() as tmpdir:
                parquet_path = Path(tmpdir) / "station_status.parquet"
                stations.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
                s3.upload_file(str(parquet_path), tgt_bucket, tgt_key)
                
        except Exception:
            logger.exception("Error loading into minio")
            raise

        return {
            "bucket": tgt_bucket,
            "key": tgt_key,
            "run_id": run_id,
        }

    @task
    def load_to_trino(load_result: dict):
        from trino.dbapi import connect
        import os

        import logging
        from airflow.hooks.base import BaseHook
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        #variables 
        schema_location = "s3a://bck-bronze/station_status/schema"
        table_location = "s3a://bck-bronze/station_status/data"

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
                schema="ecobici",
                http_scheme="http",
            )

            cursor = conn.cursor()
        except Exception:
            logger.exception("Error connecting to Trino")
            cursor.close()
            conn.close()
            raise

        create_schema_query = f"""
        CREATE SCHEMA IF NOT EXISTS bronze.ecobici
        WITH (LOCATION = '{schema_location}')
        """

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS bronze.ecobici.station_status (
            station_id varchar,
            num_bikes_available bigint,
            num_docks_available bigint,
            is_installed boolean,
            is_renting boolean,
            is_returning boolean,
            is_charging_station boolean,
            eightd_has_available_keys boolean,
            last_updated_ts timestamp(3),
            last_reported_ts timestamp(3),
            run_id varchar
        )
        WITH (
            external_location = '{table_location}',
            format = 'PARQUET',
            partitioned_by = ARRAY['run_id']
        )
        """
        try:
            cursor.execute(create_schema_query)
            cursor.execute(create_table_query)
            cursor.execute("CALL bronze.system.sync_partition_metadata('ecobici', 'station_status', 'ADD', true)")
            cursor.close()
            conn.close()
        except Exception:
            logger.exception("Error creating schema or table in Trino")
            cursor.close()
            conn.close()
            raise

    bronze_result = load_to_bronze()
    load_to_trino(bronze_result)


dag = status_stations_pipe()
