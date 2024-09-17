from airflow import DAG
import pendulum
import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_conn_test",
    start_date=pendulum.datetime(2024, 8, 12, 21, 53, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시 조민준123
) as dag:
    
    t1 = EmptyOperator(
        task_id = 't1'
    )
    
    t2 = EmptyOperator(
        task_id = 't2'
    )
    t1 >> t2 >> t1
    
