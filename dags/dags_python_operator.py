from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",  # (데일리)매일 02:30에 실행
    start_date=pendulum.datetime(2024, 8, 12, 2, 35, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rend_int = random.randint(0,3) #0~3 랜덤값
        print(fruit[rend_int])
    
    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable = select_fruit 
    )
    
    py_t1
    