# TODO import libraries
import requests
import logging

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# TODO lis of cryptocurrencies to fetch (input for dynamic task mapping)
cryptocurrencies = ["ethereum", "solana", "cardano"]
api_call_template = "https://api.coingecko.com/api/v3/simple/price?ids={crypto}&vs_currencies=usd"


@dag(
    dag_id="dtm-cryptocurrency-prices",
    schedule_interval="@daily",
    start_date=datetime(2024, 10, 18),
    catchup=False
)
def crypto_prices():

    # TODO task to dynamically map over the list of cryptocurrencies with custom index name
    @task(map_index_template="{{ crypto }}")
    def fetch_price(crypto: str):
        """fetch the price of a given cryptocurrency from the API."""

        # TODO use the cryptoncurrency name in the task name
        context = get_current_context()
        context["crypto"] = crypto

        # TODO API call to fetch the price of the cryptoncurrency
        api_url = api_call_template.format(crypto=crypto)
        response = requests.get(api_url).json()
        price = response[crypto]['usd']
        logging.info(f"the price of {crypto} is ${price}")

        return price
    
    # TODO dynamically map the fetch_price task over the list of cryptocurrencies
    princes = fetch_price.partial().expand(crypto=cryptocurrencies)

    princes


# TODO instantiate the DAG
crypto_prices()