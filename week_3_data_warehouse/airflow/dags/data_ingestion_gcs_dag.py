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
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# dataset_file = "yellow_tripdata_2021-01.csv"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
taxi_dataset_file = "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
taxi_dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_dataset_file}"

fhv_dataset_file = "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
fhv_dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{fhv_dataset_file}"


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


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


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@yearly",
    default_args=default_args,
    start_date= datetime(2019, 1, 1), 
    end_date= datetime(2022, 12, 30),
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_taxi_dataset_task = BashOperator(
        task_id="download_taxi_dataset_task",
        bash_command=f"curl -sSL {taxi_dataset_url} > {path_to_local_home}/{taxi_dataset_file}"
    )

    download_fhv_dataset_task = BashOperator(
        task_id="download_fhv_dataset_task",
        bash_command=f"curl -sSL {fhv_dataset_url} > {path_to_local_home}/{fhv_dataset_file}"
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    taxi_to_gcs_task = PythonOperator(
        task_id="taxi_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"yellow/{taxi_dataset_file}",
            "local_file": f"{path_to_local_home}/{taxi_dataset_file}",
        },
    )

    fhv_to_gcs_task = PythonOperator(
        task_id="fhv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"fhv/{fhv_dataset_file}",
            "local_file": f"{path_to_local_home}/{fhv_dataset_file}",
        },
    )


    bigquery_external_table_task_taxi = BigQueryCreateExternalTableOperator(
        task_id="taxi_bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/taxi/{taxi_dataset_file}"],
            },
        },
    )

    bigquery_external_table_task_fhv = BigQueryCreateExternalTableOperator(
        task_id="fhv_bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/fhv/{fhv_dataset_file}"],
            },
        },
    )

    # download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
    download_taxi_dataset_task >> taxi_to_gcs_task >> bigquery_external_table_task_taxi
    download_fhv_dataset_task >> fhv_to_gcs_task >> bigquery_external_table_task_fhv
