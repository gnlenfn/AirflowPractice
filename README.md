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