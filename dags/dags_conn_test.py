from airflow import DAG
import pendulum
import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_conn_test",
    start_date=pendulum.datetime(2024, 8, 12, 21, 53, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    
    t1 = EmptyOperator(
        task_id = 't1'
    )
    
    t2 = EmptyOperator(
        task_id = 't2'
    )
    
    t3 = EmptyOperator(
        task_id = 't3'
    )
    
    t4 = EmptyOperator(
        task_id = 't4'
    )
    
    t5 = EmptyOperator(
        task_id = 't5'
    )
    
    t6 = EmptyOperator(
        task_id = 't6'
    )
        
    t7 = EmptyOperator(
        task_id = 't7'
    )
        
    t8 = EmptyOperator(
        task_id = 't8'
    )
    
    t1 >> [t2, t3] >> t4
    t5 >> t4
    [t4, t7] >> t6 >> t8