import os
import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime

from airflow import DAG
from google.cloud import storage
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'

YELLOW_URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

GREEN_URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/green_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TABLE_NAME_TEMPLATE = 'green_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
GREEN_TAXI_GCS_PATH_TEMPLATE = "raw/green_tripdata/{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

FHV_URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TABLE_NAME_TEMPLATE = 'fhv_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
FHV_TAXI_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

# FHV_URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# FHV_TABLE_NAME_TEMPLATE = 'fhv_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
# FHV_TAXI_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    #"start_date": datetime(2022, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_to_gcs_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2022, 4, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=6,
    tags=['dtc-de'],
) as dag:

    wget_yellow_task = BashOperator(
        task_id='wget_yellow_task',
        bash_command=f'curl -sSLf {YELLOW_URL_TEMPLATE} > {YELLOW_OUTPUT_FILE_TEMPLATE}'
    )

    wget_green_task = BashOperator(
        task_id='wget_green_task',
        bash_command=f'curl -sSLf {GREEN_URL_TEMPLATE} > {GREEN_OUTPUT_FILE_TEMPLATE}'
    )

    wget_fhv_task = BashOperator(
        task_id='wget_fhv_task',
        bash_command=f'curl -sSLf {FHV_URL_TEMPLATE} > {FHV_OUTPUT_FILE_TEMPLATE}'
    )

    gcs_yellow_task = PythonOperator(
        task_id="gcs_yellow_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": YELLOW_TAXI_GCS_PATH_TEMPLATE,
            "local_file": YELLOW_OUTPUT_FILE_TEMPLATE,
        },
    )

    gcs_green_task = PythonOperator(
        task_id="gcs_green_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": GREEN_TAXI_GCS_PATH_TEMPLATE,
            "local_file": GREEN_OUTPUT_FILE_TEMPLATE,
        },
    )

    gcs_fhv_task = PythonOperator(
        task_id="gcs_fhv_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": FHV_TAXI_GCS_PATH_TEMPLATE,
            "local_file": FHV_OUTPUT_FILE_TEMPLATE,
        },
    )

    rm_yellow_task = BashOperator(
        task_id="rm_yellow_task",
        bash_command=f"rm {YELLOW_OUTPUT_FILE_TEMPLATE}"
    )

    rm_green_task = BashOperator(
        task_id="rm_green_task",
        bash_command=f"rm {GREEN_OUTPUT_FILE_TEMPLATE}"
    )

    rm_fhv_task = BashOperator(
        task_id="rm_fhv_task",
        bash_command=f"rm {FHV_OUTPUT_FILE_TEMPLATE}"
    )

    wget_yellow_task >> gcs_yellow_task >> rm_yellow_task
    wget_green_task >> gcs_green_task >> rm_green_task
    wget_fhv_task >> gcs_fhv_task >> rm_fhv_task

# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

ZONES_URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
ZONES_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.parquet'
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.parquet"

with DAG(
    dag_id="zones_data_dag",
    schedule_interval="@once",
    start_date=days_ago(0),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as zones_dag:

    wget_zones_task = BashOperator(
        task_id='wget_zones_task',
        bash_command=f'curl -sSLf {ZONES_URL_TEMPLATE} > {ZONES_CSV_FILE_TEMPLATE}'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": ZONES_CSV_FILE_TEMPLATE,
            "dest_file": ZONES_PARQUET_FILE_TEMPLATE
        },
    )

    gcs_zones_task = PythonOperator(
        task_id="gcs_zones_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": ZONES_GCS_PATH_TEMPLATE,
            "local_file": ZONES_PARQUET_FILE_TEMPLATE,
        },
    )

    rm_zones_task = BashOperator(
        task_id="rm_zones_task",
        bash_command=f"rm {ZONES_CSV_FILE_TEMPLATE} {ZONES_PARQUET_FILE_TEMPLATE}"
    )

    wget_zones_task >> format_to_parquet_task >> gcs_zones_task >> rm_zones_task