from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",  # 매일 00:10에 실행
    start_date=pendulum.datetime(2023, 3, 1, 00, 00, tz="Asia/Seoul"),  # 2023년 3월 1일 00:00부터 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command = "echo START && "
                       "echo XCOM_PUSHED "
                       "{{ti.xcom_push(key = 'bash_pushed', value = 'first_bash_message')}} && "
                       "echo COMPLETE"
    )

    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env = {'PUSED_VALUE' : "{{ti.xcom_pull(key = 'bash_pushed')}}",
               'RETURN_VALUE' : "{{ti.xcom_pull(task_ids = 'bash_push')}}"
               },
        bash_command = "echo $PUSHED_VALUE &&*echo $RETURN_VALUE",
        do_xcom_push = False
    )       
    
    bash_push >> bash_pull
