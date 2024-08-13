from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1",  # 0시 10분 6<토요일 #1 매월 첫주
    start_date=pendulum.datetime(2024, 8, 13, 20, 00, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    
    t1_orange = BashOperator(
        task_id = 't1_orange'
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )
    
    t2_avocado = BashOperator(
        task_id = 't2_avocado'
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh avocado",
    )
    
    t1_orange >> t2_avocado
