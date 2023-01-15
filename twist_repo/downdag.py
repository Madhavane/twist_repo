from airflow import DAG 
from airflow.utils.dates import days_ago
from datetime import date,timedelta,datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator

def print_task_type(**kwargs):
    print(f"The {kwargs['task_id']} task has completed.")

with DAG(
    dag_id = "downstream_dag",
    default_args = {"owner": "airflow"},
    start_date = days_ago(1),
    schedule_interval='@once',
    tags=['dag dependency example'],
    
    

) as dag:

    task1 = PythonOperator(
        task_id = 'task1',
        python_callable=print_task_type,
        op_kwargs={'task_id':'task1'}


    )

task1