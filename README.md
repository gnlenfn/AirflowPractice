# Practice Airflow
1. workflow.py  
파일 생성 > 파일 읽기 > 파일 제거 
기본적인 bash command 실행

2. file_sensor.py  
sensor 작동 확인

3. scrape_price.py  
coin gecko API를 사용해 5분마다 비트코인 가격을 불러오는 작업

4. elastic_scheduler.py  
elasticsearch에서 데이터를 읽어와서 PostgreSQL로 저장하는 워크플로우

5. sentiment_workflow.py  
영화 리뷰 감성분석 과정을 머신러닝으로 스케쥴링하는 작업  
특정 조건을 추가하여 분기를 생성할 수 있음  
더 복잡한 DAG 구성 가능

# Celery Executor
실제 서비스에서는 Sequential Executor와 SQLite3를 쓰면 안되기 때문에 다른 executor를 사용해야 한다.  
또한 MySQL을 사용할 경우 병렬적으로 task를 수행할 수 없다고 한다.  
  
그래서 메타DB는 postgresql을 사용하고 RabbitMQ와 함께 CeleryExecutor를 사용하여 클러스터를 구성해보았다.

![celery](https://github.com/gnlenfn/AirflowPractice/image/celery.png)

- postgresql는 마스터 노드에 설치하고 각 노드에는 모두 airflow를 설치
- 마스터 노드에서는 scheduler, webserver를 실행
- worker 노드에서는 worker를 실행
    - 노드 3개를 사용했고 마스터 노드에서도 worker를 실행
    - 각각 서비스 파일을 작성하여 데몬 실행
- `airflow.cfg` 설정 파일에서 메타DB 설정과 브로커 역할을 할 RabbitMQ 설정 등을 해주면 완료
