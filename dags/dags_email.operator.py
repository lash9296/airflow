from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email.operator",
    schedule="0 8 1 * *",  # 매일 02:30에 실행
    start_date=pendulum.datetime(2024, 8, 12, 21, 53, tz="Asia/Seoul"),  # 2024년 8월 12일 02:30 시작
    catchup=False,  # 과거 실행을 무시
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'tjrrjwu92@gmail.com',
        # cc = 참조메일
        subject= 'Airflow 성공 메일',
        html_content= 'Airflow 작업이 완료되었습니다.'
    )