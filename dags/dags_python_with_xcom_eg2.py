from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task


with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",  # 매일 06:30에 실행
    start_date=pendulum.datetime(2023, 3, 1, 00, 00, tz="Asia/Seoul"),  # 2023년 3월 1일 00:00부터 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'

    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값' + value1)

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받ㄷ은 값' + status)
    
    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()
        
