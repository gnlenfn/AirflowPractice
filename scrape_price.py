from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import requests
import logging


def extract_price():
    url = f"https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&\
                include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"
    print(url)
    return requests.get(url).json()


def process_data(**context):
    response = context['task_instance'].xcom_pull(task_ids=f'extract_bitcoin_price')
    response = response['bitcoin']
    logging.info(response)
    return {'usd': response['usd'], 'change': response['usd_24h_change']}


def store_data(**context):
    data = context['task_instance'].xcom_pull(task_ids="process_data")
    logging.info(f"Store: {data['usd']} with change {data['change']}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2022, 1, 17),
    "provide_context": True
}

with DAG(
    'price_tracking',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    max_active_runs=3
) as dag:
    extract = PythonOperator(
        task_id=f'extract_bitcoin_price',
        python_callable=extract_price,
        dag=dag
    )

    process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        op_kwargs={

        }
    )

    store = PythonOperator(
        task_id='saving_data',
        python_callable=store_data
    )

    extract >> process >> store
