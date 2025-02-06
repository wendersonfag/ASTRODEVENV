""""
Cron [Preset Schedule Expressions]
@once: Run the DAG once as soon as the DAG is triggered.
@hourly: Run the DAG every hour (0 * * * *).
@daily: Run the DAG every day at midnight (0 0 * * *).
@twice_daily: Run the DAG every day at 12:00 AM and 12:00 PM (0 0,12 * * *).
@midnight: Run the DAG once a day at midnight (0 0 0 * *).
@weekly: Run the DAG once a week on Sunday at midnight (0 0 * * 0).
@monthly: Run the DAG once a month on the first day of the month at midnight (0 0 1 * *).
@yearly or @annually: Run the DAG once a year on January 1st at midnight (0 0 1 1 *).
"""

import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.timetables.base import DagRunInfo, DataInternal, Timetable

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

class BlackFridayTimetable(Timetable)




# verificando esse está atualiando pelo pycharm
@dag(
    dag_id="schedule-cron",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    timetable=BlackFridayTimetable(),
)
def main():
    # Primeira maneira de fazer o DAG
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

main()
