from airflow import DAG
import pendulum 
from airflow.decorators import task  


with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",  # 매월 10:00에  실행
    start_date=pendulum.datetime(2023, 3, 1, 00, 00, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시(테스트)
) as dag: 
    # START_DATE : 2주전 월요일 , END_DATE : 2주전 토요일123
    @task(
        task_id = 'task_using_macros',
        template_dict ={'start_date' : '{{(data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(month = -1, day =1)) | ds}}',# 한달전 
              'end_date' : '{{(data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(days =-1)) | ds}}' # 마지막 하루전
        }
    )
    def get_datetiome_macro(**kwargs):
        template_dict = kwargs.get('templates_dict') or {}
        if template_dict:
            start_date = template_dict.get('start_date') or "start_date_없음"
            end_date = template_dict.get('end_date') or "end_date_없음"
            print(start_date)
            print(end_date)
            
    @task(
        task_id = 'task_direct_calc'
    )
    def get_datetiome_calc(**kwargs):
        from dateutil.relativedelta import relativedelta
        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(month = -1, day =1)
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(day = 1) + relativedelta(days =-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))
    
    get_datetiome_macro() >> get_datetiome_calc()

    
    
    
   
    

