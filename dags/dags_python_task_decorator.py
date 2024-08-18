from airflow import DAG
import pendulum
from airflow.decorators import dag, task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",  # 매일 02:30에 실행
    start_date=pendulum.datetime(2024, 8, 12, 2, 35, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)
    
    python_task_1 = print_context('task_decorator 실행')
