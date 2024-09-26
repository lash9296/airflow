from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task


with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",  # 매일 06:30에 실행
    start_date=pendulum.datetime(2023, 3, 1, 00, 00, tz="Asia/Seoul"),  # 2023년 3월 1일 00:00부터 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key = 'result1', value = 'value_1')
        ti.xcom_push(key = 'result2', value = [1, 2, 3])

    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key = 'result1', value = 'value_2')
        ti.xcom_push(key = 'result2', value = [1, 2, 3, 4])

    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key = 'result1') ## value_2
        value2 = ti.xcom_pull(key = 'result2', task_ids = 'python_xcom_push_task1') ##[1, 2, 3]
        print(value1)
        print(value2)
        
    xcom_push1() >> xcom_push2() >> xcom_pull()