from airflow.decorators import dag, task
from elasticsearch import Elasticsearch

from datetime import datetime, timedelta
import psycopg2

es = Elasticsearch("host",
                   http_auth=('user', 'password'),
                   port='port'
                   )

conn = psycopg2.connect(host='localhost', dbname='postgres',
                        user='user', password='password', port='port')


default_args = {
    "owner": 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 20),
}


@dag(default_args=default_args, schedule_interval=timedelta(hours=1), catchup=False)
def elasticflow():

    @task(task_id='read_data')
    def read_from_elasticsearch():
        res = es.search(index='sym-metric-network', body={
                                                  "size": 0,
                                                  "query": {
                                                    "bool": {
                                                      "must": [{
                                                        "range": {
                                                          "@timestamp": {
                                                            "gte": "now-10m/m",
                                                            "lte": "now/m"
                                                          }
                                                        }
                                                      }]
                                                    }
                                                  },
                                                  "aggs": {
                                                    "products": {
                                                      "terms": {
                                                        "field": "host.hostname.keyword",
                                                        "size": 10000,
                                                        "order": {"_term": "asc"}
                                                      }
                                                    }
                                                  }
                                                }
                        )
        print(res)
        return res['aggregations']['products']

    @task(task_id='transform')
    def process_data(response):
        response = response['buckets']
        return {"k1": response[0], "k2": response[1], "k3": response[2], "k4": response[3]}

    @task(task_id='logging')
    def save_on_postgre(response):
        with conn:
            with conn.cursor() as cur:
                for target in response.values():
                    cur.execute(f"select * from test where index_name='{target['key']}';")
                    rows = cur.fetchall()
                    if rows:
                        print("Data already exists!")
                    else:
                        cur.execute(f"insert into test values(default, '{target['key']}');")

        conn.commit()
        conn.close()

    save_on_postgre(process_data(read_from_elasticsearch()))


dag = elasticflow()
