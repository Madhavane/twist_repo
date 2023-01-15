from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from datetime import timedelta
from airflow.utils.dates import days_ago
default_args = {
    'owner': 'default_user',
    'start_date': days_ago(2),
    'depends_on_past': True,
    # With this set to true, the pipeline won't run if the previous day failed
    #'email': ['demo@email.de'],
    #'email_on_failure': True,
    # upon failure this pipeline will send an email to your email set above
    #'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=30),
}

with DAG('dependencies',schedule_interval='@daily',default_args=default_args) as dag:
    t0 = EmptyOperator(task_id='t0')
    t1 = EmptyOperator(task_id='t1')
    t2 = EmptyOperator(task_id='t2')
    t3 = EmptyOperator(task_id='t3')
    t4 = EmptyOperator(task_id='t4')
    t5 = EmptyOperator(task_id='t5')
    t6 = EmptyOperator(task_id='t6')
    t7 = EmptyOperator(task_id='t7')
    t8 = EmptyOperator(task_id='t8')


chain(t0,t1,[t2,t3],[t4,t5],t6)
