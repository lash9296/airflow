from airflow import DAG
import pendulum 
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_pyrhon_template",
    schedule="30 9 * * *",  # 매일 09:30에 실행
    start_date=pendulum.datetime(2024, 9, 19, 17, 00, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시(테스트)
) as dag: 
    
    def python_function1(start_date, end_date, **kwargs):# op_kwargs에 변수를 직접 줘서 긁어오는 방식
        print(start_date)
        print(end_date)
    
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = python_function1,
        op_kwargs={'start_date' : '{{data_interval_start | ds}}', 'end_date' : '{{data_interval_end | ds}}'}
    )

    @task(task_id = 'python_2')# kwargs에서 꺼내쓰는 방식
    def python_function2(**kwargs):
        print(kwargs)
        print('ds' + kwargs['ds'])
        print('ts' + kwargs['ts'])
        print('data_interval_start' + str(kwargs['data_interval_start']))
        print('data_interval_end' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))
        
    python_t1 >> python_function2()

         
    
    
   
    

