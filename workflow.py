from airflow import models
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with models.DAG(
    dag_id='echo_test',
    description="echo test",
    max_active_runs=5,
    schedule_interval=None,
    concurrency=10,
    default_args=default_args
) as dag:

    text_file_path = "/Users/honginyoon/airflow/dags"

    # create txt file
    create_text_file_command = f"cd {text_file_path} && echo hello airflow > test.txt"
    create_text_file = BashOperator(
        task_id="create_text_file",
        bash_command=create_text_file_command,
        dag=dag
    )

    # cat txt file
    read_text_file_command = f'cd {text_file_path} && cat test.txt'
    read_text_file = BashOperator(
        task_id='cat_text_file',
        bash_command=read_text_file_command,
        dag=dag
    )

    # remove txt file
    remove_text_file_command = f'cd {text_file_path} && rm test.txt'
    remove_text_file = BashOperator(
        task_id='remove_text_file',
        bash_command=remove_text_file_command,
        dag=dag
    )

    create_text_file >> read_text_file >> remove_text_file
