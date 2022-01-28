import random

import keras.preprocessing.sequence
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import psycopg2

from models import toy

es = Elasticsearch("host",
                   http_auth=('user', 'password'),
                   port='port'
                   )

conn = psycopg2.connect(host='localhost', dbname='postgres',
                        user='user', password='password', port='port')

default_args = {
    'onwer': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2022, 1, 25),
    "provide_context": True,
}


def data_loading():
    train_data, test_data, train_labels, test_labels = toy.data_load()

    return {"train_data": train_data, "test_data": test_data,
            "train_labels": train_labels, "test_labels": test_labels}


options = ['build_model', 'evaluate']

# test데이터인지 train데이터인지 선택
def which_path():
    branches = [True, False]
    flag = random.choice(branches)
    print(flag)
    if flag:
        task_id = 'build_model'
    else:
        task_id = 'evaluate'
    return task_id


def preprocess():
    imdb = keras.datasets.imdb
    word_index = imdb.get_word_index()
    word_index = {k: (v + 3) for k, v in word_index.items()}
    word_index["<PAD>"] = 0
    word_index["<START>"] = 1
    word_index["<UNK>"] = 2  # unknown
    word_index["<UNUSED>"] = 3
    train_data, test_data, train_label, test_label = toy.data_load()

    train_data = keras.preprocessing.sequence.pad_sequences(train_data,
                                                            value=word_index["<PAD>"],
                                                            padding='post',
                                                            maxlen=256)
    test_data = keras.preprocessing.sequence.pad_sequences(test_data,
                                                           value=word_index["<PAD>"],
                                                           padding='post',
                                                           maxlen=256)
    return {"train_data": train_data, "test_data": test_data}


def training(**context):
    model = toy.build_model()
    train_data = context['task_instance'].xcom_pull(task_ids='preprocess')['train_data']
    train_label = context['task_instance'].xcom_pull(task_ids='data_load')['train_labels']
    toy.model_training(model, train_data, train_label)


def test_model(**context):
    test_data = context['task_instance'].xcom_pull(task_ids='preprocess')['test_data']
    test_label = context['task_instance'].xcom_pull(task_ids='data_load')['test_labels']
    result = toy.evaluate_model(test_data, test_label)

    return {"loss": result[0], "score": result[1]}


def save_on_elasticsearch(**context):
    flag = context['task_instance'].xcom_pull(task_ids='check_cond')
    score = context['ti'].xcom_pull(task_ids='evaluate')['score']
    loss = context['ti'].xcom_pull(task_ids='evaluate')['loss']

    if flag == 'evaluate':  # test
        res = es.index(index="ml_test_result", body={"score": score, "loss": loss})
    else:
        res = es.index(index='ml_train_result', body={"score": score, "loss": loss})


# 어느 db에 저장할 지 선택 --> test는 es로, train은 postresql로
def save_where(**context):
    flag = context['ti'].xcom_pull(task_ids='check_cond')
    if flag == 'build_model':
        task_id = 'create_post_table'  # train
    else:
        task_id = 'es_insert'          # test
    return task_id


def postres_insert(**context):
    score = context['ti'].xcom_pull(task_ids='evaluate')['score']
    sql = f"INSERT INTO sentiment VALUES (default, {score});"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)


with DAG('sentiment_analysis_to_db', default_args=default_args,
         schedule_interval=timedelta(hours=1), catchup=False) as dag:

    data_load = PythonOperator(
        task_id='data_load',
        python_callable=data_loading,
        dag=dag
    )

    check_condition = BranchPythonOperator(
        task_id='check_cond',
        python_callable=which_path,
        dag=dag
    )

    prep = PythonOperator(
        task_id='preprocess',
        python_callable=preprocess,
        dag=dag
    )

    train_model = PythonOperator(
        task_id='build_model',
        python_callable=training,
        trigger_rule='one_success',
        dag=dag
    )

    evaluate = PythonOperator(
        task_id='evaluate',
        python_callable=test_model,
        trigger_rule='one_success',
        dag=dag
    )

    data_load >> prep >> check_condition
    for opt in options:
        if opt == 'build_model':
            check_condition >> train_model >> evaluate
        else:
            check_condition >> evaluate

    create_table = PostgresOperator(
        task_id="create_post_table",
        postgres_conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS sentiment (
            id SERIAL PRIMARY KEY,
            score FLOAT NOT NULL
            );
        """
    )

    insert_postgre = PythonOperator(
        task_id='postgre_insert',
        python_callable=postres_insert,
        dag=dag
    )

    insert_postgre2 = PythonOperator(
        task_id='postgre_insert2',
        python_callable=postres_insert,
        dag=dag
    )

    insert_es = PythonOperator(
        task_id="es_insert",
        python_callable=save_on_elasticsearch,
        dag=dag
    )

    check_save = BranchPythonOperator(
        task_id='check_save_cond',
        python_callable=save_where,
        dag=dag
    )

    evaluate >> check_save
    options = ['create_post_table', 'es_insert']
    for opt in options:
        if opt == 'create_post_table':
            check_save >> create_table >> insert_postgre
        else:
            check_save >> insert_es

    # test결과가 score 0.5이상히면 postres에 추가로 저장
    def over_threshold(**context):
        score = context['ti'].xcom_pull(task_ids='evaluate')['score']
        if score >= 0.5:
            task_id = 'postgre_insert2'
        else:
            task_id = 'dummy'
        return task_id

    threshold = BranchPythonOperator(
        task_id='threshold',
        python_callable=over_threshold,
        dag=dag
    )

    dummy = DummyOperator(
        task_id='dummy',
        trigger_rule='one_success',
        dag=dag
    )

    sample = DummyOperator(
        task_id="end_task",
        trigger_rule='one_success',
        dag=dag
    )
    insert_es >> threshold
    options = ['postgre_insert2', 'dummy', 'end_task']
    for opt in options:
        if opt == 'dummy':
            threshold >> dummy
        elif opt == 'end_task':
            threshold >> sample
        else:
            threshold >> insert_postgre2

