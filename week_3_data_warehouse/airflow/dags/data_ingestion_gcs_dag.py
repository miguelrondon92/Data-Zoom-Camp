import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd 
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# dataset_file = "yellow_tripdata_2021-01.csv"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
date_time_eop = "_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
taxi_dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

fhv_schema = pass 
yellow_schema = pass 
green_schema = pass 

taxi_types = ["yellow", "green", "fhv"]

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def transform_schema(parquet_file, taxi):
    """
    give implicit schema to parquet files 
    """
    pd.read_parquet(parquet_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    start_date= datetime(2019, 1, 1), 
    end_date= datetime(2021, 12, 30),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:
    for taxi in taxi_types:

        # echo_taxi = BashOperator(
        #     task_id=f"echo_{taxi}_path",
        #     bash_command=f"echo {taxi_dataset_url}{taxi}{date_time_eop}"
        # )

        download_taxi_dataset_task = BashOperator(
            task_id=f"download_{taxi}_dataset_task",
            bash_command=f"curl -sSL {taxi_dataset_url}{taxi}{date_time_eop} > {path_to_local_home}/{taxi}{date_time_eop}"
        )

        change_schema = PythonOperator(
            task_id = f"{taxi}_schema_change",
            python_callable=transform_schema 
        )
        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        taxi_to_gcs_task = PythonOperator(
            task_id=f"{taxi}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"{taxi}/{taxi}{date_time_eop}",
                "local_file": f"{path_to_local_home}/{taxi}{date_time_eop}",
            },
        )

        bigquery_external_table_task_taxi = BigQueryCreateExternalTableOperator(
            task_id=f"{taxi}_bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": "external_table",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/{taxi}/{taxi}{date_time_eop}"],
                },
            },
        )

        # download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
        download_taxi_dataset_task >> taxi_to_gcs_task >> bigquery_external_table_task_taxi
