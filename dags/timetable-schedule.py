import logging
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from airflow.timetables.registry import register_timetable

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

class BlackFridayTimetable(Timetable): 
    """
    Custom Timetable for normal Daily Execution but increased frequency on Black Friday
    """

    def is_black_friday(self, current_date: datetime) -> bool:
        if current_date.month == 11 and current_date.weekday() == 4:
            last_day_of_november = current_date.replace(day=30)
            while last_day_of_november.weekday() != 4:
                last_day_of_november -= timedelta(days=1)
            return current_date.day == last_day_of_november.day
        return False

    def next_dagrun_info(self, *, last_automated_data_interval: DataInterval, restriction) -> DagRunInfo:
        next_start = last_automated_data_interval.end if last_automated_data_interval else datetime.now()

        if self.is_black_friday(next_start):
            next_end = next_start + timedelta(hours=1)
        else:
            next_start = next_start.replace(hour=9, minute=0, second=0, microsecond=0)
            next_end = next_start + timedelta(days=1)

        return DagRunInfo.interval(start=next_start, end=next_end)

register_timetable("black_friday_timetable", BlackFridayTimetable)

@dag(
    dag_id="schedule-cron",
    start_date=datetime(2025, 2, 1),
    catchup=False,
    timetable=BlackFridayTimetable(),
)
def main():
    transform = TaskGroup("transform")
    store = TaskGroup("store")

    @task(task_id="extract", retries=2, task_group=transform)
    def extract_bitcoin():
        return requests.get(API).json()["bitcoin"]

    @task(task_id="transform", task_group=transform)
    def process_bitcoin(response):
        return {
            "usd": response["usd"],
            "change": response["usd_24h_change"]
        }

    process_data = process_bitcoin(extract_bitcoin())

    @task(task_id="store", task_group=store)
    def store_bitcoin(data):
        logging.info(f"Bitcoin price: {data['usd']}, change: {data['change']}")

    store_bitcoin(process_data)

main()