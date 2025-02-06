import logging
import requests

from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup



API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

#verificando esse está atualiando pelo pycharm
@dag(
    dag_id="tf-bitcoin",
    schedule="@daily",
    start_date=datetime(2025, 2, 1),
    catchup=False
)

def main():
    #Primeira maneira de fazer o DAG
    # TODO TASKGROUP

    transform = TaskGroup("transform")
    store = TaskGroup("store")

    # TODO TASK 1
    @task(task_id="extract", retries=2, task_group=transform)
    def extract_bitcoin():
        return requests.get(API).json()["bitcoin"]

     # TODO TASK 2
    @task(task_id="transform", task_group=transform)
    def process_bitcoin(response):
       return {
              "usd": response["usd"],
              "change": response["usd_24h_change"]
        }
    
    # TODO Dependencies
    process_data = process_bitcoin(extract_bitcoin())
    
    # TODO TASK 3
    @task(task_id="store", task_group=store)
    def store_bitcoin(data):
        logging.info(f"Bitcoin price: {data['usd']}, change: {data['change']}")

    # TODO Dependencies
    store_bitcoin(process_data)

    #Segunda maneira de fazer o DAG
    # TODO TASKGROUP
    #with TaskGroup("transformers") as transformers:
    

        # TODO TASK 1
     #   @task(task_id="extract", retries=2)
     #   def extract_bitcoin():
     #       return requests.get(API).json()["bitcoin"]
        

        # TODO TASK 2
     #   @task(task_id="transform")
     #   def process_bitcoin(response):
     #       return {
     #           "usd": response["usd"],
     #           "change": response["usd_24h_change"]
     #       }

        
        # TODO Dependencies
     #   process_data = process_bitcoin(extract_bitcoin())
    
    #with TaskGroup("store") as stores:
        # TODO TASK 3
    #    @task(task_id="store")
    #   def store_bitcoin(data):
    #       logging.info(f"Bitcoin price: {data['usd']}, change: {data['change']}")

    #    store_bitcoin(process_data)

### executando a função maindd

main() 
