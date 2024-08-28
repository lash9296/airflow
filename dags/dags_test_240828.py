from airflow import DAG
import pendulum 
import datetime
from airflow.operators.bash import BashOperator
with DAG(
    dag_id="dags_python_with_op_kwarg",
    schedule="0 2 * * *",  # 매일 02:30에 실행
    start_date=pendulum.datetime(2024, 8, 12, 2, 35, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시(테스트)
) as dag: 
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command= 'echo "data_interval_end : {{data_interval_end}}"'
    )
    
    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env={
            'Start_Date :' :'{{data_interval_start | ds}}',
            'End_Date :' :'{{data_interval_end | ds}}'
        },
        bash_command= 'echo $Start_date && echo $End_date' # && 앞에있는것이 성공하면 뒤에껄 주겟다 
       
    )
    
    bash_t1 >> bash_t2

         
    
    
   
    

