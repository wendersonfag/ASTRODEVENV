"""
DAG Skeleton

- Imports
- Connections & Variables
- DataSets
- Default Arguments
- DAG Definition
- Task Declaration
- Task Dependencies
- DAG Instantiation

python cli.py atlas
"""

# TODO Imports
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# TODO Datasets
users_parquet_dataset = Dataset("bigquery://OwsHQ.users")
payments_parquet_dataset = Dataset("bigquery://OwsHQ.payments")

# TODO Connections & Variables
airbyte_conn_id = "airbyte_default"
airbyte_sync_atlas_gcs_id = "f402dd35-6199-4001-b949-1d356411eb8f"
landing_zone_path = "gs://owshq-airbyte-ingestion2/"
source_gcs_conn_id = "google_cloud_default"
bq_conn_id = "google_cloud_default"

# TODO Default Arguments
default_args = {
    "owner": "Wenderson Fagundes",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# TODO DAG Definition
@dag(
    dag_id="airbyte-sync-astro-sdk-bq",
    start_date=datetime(2024, 9, 26),
    max_active_runs=1,
    schedule_interval=timedelta(hours=8),
    default_args=default_args,
    catchup=False,
    tags=['development', 'ingestion', 'airbyte', 'postgres', 'mongodb', 'gcs']
)
def init():

    # TODO Tasks Declaration
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    trigger_airbyte_sync_atlas_gcs = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        connection_id=airbyte_sync_atlas_gcs_id,
        airbyte_conn_id=airbyte_conn_id,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    users_parquet = aql.load_file(
        task_id="users_parquet",
        input_file=File(path=landing_zone_path + "mangodb-atlas/payments", filetype=FileType.PARQUET, conn_id=source_gcs_conn_id),
        output_table=Table(name="users", metadata=Metadata(schema="OwsHQ", database="master-inn-450813-t1"), conn_id=bq_conn_id),
        if_exists="replace",
        use_native_support=True,
        outlets=[users_parquet_dataset]
    )

    payments_parquet = aql.load_file(
        task_id="payments_parquet",
        input_file=File(path=landing_zone_path + "mangodb-atlas/users", filetype=FileType.PARQUET, conn_id=source_gcs_conn_id),
        output_table=Table(name="payments", metadata=Metadata(schema="OwsHQ", database="master-inn-450813-t1"), conn_id=bq_conn_id),
        if_exists="replace",
        use_native_support=True,
        outlets=[payments_parquet_dataset]
    )

    # TODO Task Dependencies
    #start >> trigger_airbyte_sync_atlas_gcs >> end
    start >> trigger_airbyte_sync_atlas_gcs >> [users_parquet, payments_parquet] >> end


# TODO DAG Instantiation
dag = init()