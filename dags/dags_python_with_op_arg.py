from airflow import DAG
import pendulum 
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from common.common_func import regist
with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * *",  # 매일 02:30에 실행
    start_date=pendulum.datetime(2024, 8, 12, 2, 35, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    
    regist_t1 = PythonOperator(
        task_id = 'regist_t1',
        python_callable=regist,
        op_args=['mjcho','man','kr','goyang']
    )

    regist_t1
    

