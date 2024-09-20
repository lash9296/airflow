from airflow import DAG
import pendulum 
from airflow.operators.bash import BaseOperator


with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 * * 6#2",  # 매월 둘째주#2 토요일(6) 10:00에  실행
    start_date=pendulum.datetime(2024, 9, 19, 17, 00, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시(테스트)
) as dag: 
    # START_DATE : 2주전 월요일 , END_DATE : 2주전 토요일
    bash_task_2 = BaseOperator(
        task_id = 'bash_task_2',
        env ={'START_DATE' : '{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days = 19)) | ds}}',# 2주전 월
              'END_DATE' : '{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days = 14)) | ds}}' # 2주전 토
        },
        bash_command = 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE'
    )

         
    
    
   
    

