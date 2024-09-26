from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.macros import ds_format

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",  # 매일 00:10에 실행
    start_date=pendulum.datetime(2023, 3, 1, 00, 00, tz="Asia/Seoul"),  # 2023년 3월 1일 00:00부터 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    # START_DATE : 한 달 전 첫째 날, END_DATE : 한 달 전 마지막 날
    @task(
        task_id='task_using_macros',
        templates_dict={
            'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',  # 한 달 전 첫째 날
            'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'  # 마지막 하루 전
        }
    )
    def get_datetime_macro(templates_dict=None, **kwargs):
        start_date = templates_dict.get('start_date', "start_date_없음")
        end_date = templates_dict.get('end_date', "end_date_없음")
        print(start_date)
        print(end_date)

    @task(
        task_id='task_direct_calc'
    )
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta
        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day=1)  # 한 달 전 첫째 날 계산
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)  # 한 달 전 마지막 날 계산
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()
