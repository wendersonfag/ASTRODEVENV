import requests
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

def extract_bitcoin():
    return requests.get(API).json()["bitcoin"]

def process_bitcoin(ti):
    response = ti.xcom_pull(task_ids="extract_bitcoin_from_api")
    logging.info(response)
    processed_data = {"usd": response["usd"], "change": response["usd_24h_change"]}
    ti.xcom_push(key="processed_data", value=processed_data)

def store_bitcoin(ti):
    # Corrigido a key para "processed_data"
    data = ti.xcom_pull(task_ids="process_bitcoin_from_api", key="processed_data")
    logging.info(data)

with DAG(
    dag_id="cls_bitcoin",  # Removido o "_test" e trocado hífen por underscore
    schedule="@daily",
    start_date=datetime(2025, 2, 1),
    catchup=False
) as dag:

    extract_bitcoin_from_api = PythonOperator(  
        task_id="extract_bitcoin_from_api",
        python_callable=extract_bitcoin
    )

    process_bitcoin_from_api = PythonOperator(
        task_id="process_bitcoin_from_api",
        python_callable=process_bitcoin
    )

    store_bitcoin_from_api = PythonOperator(   
        task_id="store_bitcoin_from_api",
        python_callable=store_bitcoin
    )
    
    # TODO Dependencies
    extract_bitcoin_from_api >> process_bitcoin_from_api >> store_bitcoin_from_api 