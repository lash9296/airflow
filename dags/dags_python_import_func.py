from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import random
from common.common_func import ger_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",  # (데일리)매일 02:30에 실행
    start_date=pendulum.datetime(2024, 8, 12, 2, 35, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시
) as dag:

    task_get_sftp = PythonOperator(
        task_id = 'task_get_sftp',
        python_callable = ger_sftp 
    )
