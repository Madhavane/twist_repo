from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
dbt_verb='run'

dag = DAG(
    dag_id='dbt_dag1',
    start_date=datetime(2020, 12, 23),
    description='A dbt wrapper for Airflow',
    schedule_interval=timedelta(days=1),
)

def load_manifest():
    local_filepath = "/home/madhav/dbt_pro/twist_dbt/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)

    return data

def make_dbt_task(model, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    DBT_DIR = "/usr/local/airflow/dags/dbt"
    GLOBAL_CLI_FLAGS = "--no-write-json"
    #--model = node.split(".")[-1]

   
    dbt_task = BashOperator(
            task_id=model,
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --select {model}
            """,
            dag=dag,
        )

    
    return dbt_task

data = load_manifest()

make_dbt_task('salesforce',dbt_verb) >> make_dbt_task('awesome',dbt_verb)