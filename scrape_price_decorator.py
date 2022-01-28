from airflow.decorators import dag, task

from datetime import datetime, timedelta
import requests
import logging

default_args = {
    'onwer': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2022, 1, 17),
    "provide_context": True
}


@dag(default_args=default_args, schedule_interval=timedelta(minutes=5), catchup=False)
def taskflow():

    @task(task_id=f'extract', retries=3)
    def extract_price():
        url = f"https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&\
                    include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

        return requests.get(url).json()

    @task(task_id='process', multiple_outputs=True)
    def process_data(response):
        data = response['bitcoin']
        logging.info(data)
        return {'usd': data['usd'], 'change': data['usd_24h_change']}

    @task(task_id='store')
    def store_data(data):
        logging.info(f"Store: {data['usd']} with change {data['change']}")

    store_data(process_data(extract_price()))


dag = taskflow()
