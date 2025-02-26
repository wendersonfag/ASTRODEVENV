"""
DAG Skeleton

- Importas
- Connections & Variables
- DataSets
- Default Arguments
- DAG Definition
- Task Declaration
- Task Dependencies
- DAG Instatiation
"""

# TODO Imports
import json
import tempfile
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.datasets import Dataset
from typing import List, Dict

# TODO Connections & Variables
GCS_CONN_ID = "google_cloud_default"
BIGQUERY_CONN_ID = "google_cloud_default"
BUCKET_SOURCE = "owshq-uber-eats-files"

# ToDO Datasets [Outlets]
user_dataset = Dataset(f"gs://{BUCKET_SOURCE}/users/")
order_dataset = Dataset(f"gs://{BUCKET_SOURCE}/orders/")

# TODO Default Arguments
default_args = {
    "owner" : "Wenderson Fagundes",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#TODO DAG Definition
@dag(
    dag_id="prd-users-orders-sensor-transformer",
    start_date=datetime(2024, 9, 24),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=5),
    default_args=default_args,
    catchup=False,
    #owner_links={""}
    tags=['development', 'elt', 'gcs', 'files']
)
def init():

    # TODO Task Declaration
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    #TODO Task Dependecies
    start >> end


# TODO DAG Instatiation
dag = init()
