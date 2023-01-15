# DAG CREATED BY PYTHON SCRIPT -> 2022-12-12 09:41:24.011168
from datetime import datetime
import logging
import sys
from airflow.models import DAG
from airflow.providers.docker import DockerOperator
from airflow.operators.python import PythonOperator


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
tag_list = ["DBT", "twist-etl"]
default_args = {
    'start_date': datetime(2021, 1, 1),
    'owner': 'Admin'
}
DOCKER_IMAGE = "dbt-jaffle-shop:latest"
GLOBAL_CLI_FLAGS = "--no-write-json --no-use-colors" #prevents DBT from writing a new manifest.json file and remove colors from logs
def run_dbt_task(subtask_id="", model_path="", is_test=False, **kwargs):
    if kwargs.get('dag_run').conf:
        DBT_TARGET = kwargs.get('dag_run').conf.get("DBT_TARGET", "dev")
        FULL_REFRESH = kwargs.get('dag_run').conf.get("FULL_REFRESH", False)
    else:
        DBT_TARGET = "dev"
        FULL_REFRESH = False
    print(f"DBT_TARGET -> {DBT_TARGET}\nFULL_REFRESH -> {FULL_REFRESH}")
    dbt_command = "run"
    if is_test:
        dbt_command = "test"
    elif FULL_REFRESH:
        dbt_command = "run --full-refresh"
    dbt_task = DockerOperator(
            task_id=subtask_id,
            image=DOCKER_IMAGE,
            api_version='auto',
            auto_remove=True,
            command=f"dbt {GLOBAL_CLI_FLAGS} {dbt_command} --profiles-dir profile --target {DBT_TARGET} --models {model_path}",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            tty=True
        )
    dbt_task.execute(dict())
    
with DAG('DBT-twist_run', schedule_interval='@daily', default_args=default_args, tags=tag_list, catchup=False) as dag:

    sales_TDW_OLIGO_CONTROL_BOIA = PythonOperator(
        task_id="sales_TDW.OLIGO_CONTROL_BOIA",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.OLIGO_CONTROL_BOIA", "model_path": "awesome.OLIGO_CONTROL_BOIA", "is_test": False}
    )

    sales_TDW_OLIGO_CONTROL_DEVICE_READ_BINS = PythonOperator(
        task_id="sales_TDW.OLIGO_CONTROL_DEVICE_READ_BINS",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.OLIGO_CONTROL_DEVICE_READ_BINS", "model_path": "awesome.OLIGO_CONTROL_DEVICE_READ_BINS", "is_test": False}
    )

    sales_TDW_OLIGO_CONTROL_MUTATIONS_PER_POSITION = PythonOperator(
        task_id="sales_TDW.OLIGO_CONTROL_MUTATIONS_PER_POSITION",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.OLIGO_CONTROL_MUTATIONS_PER_POSITION", "model_path": "awesome.OLIGO_CONTROL_MUTATIONS_PER_POSITION", "is_test": False}
    )

    sales_TDW_OLIGO_CONTROL_MUTATION_PER_CLUSTER = PythonOperator(
        task_id="sales_TDW.OLIGO_CONTROL_MUTATION_PER_CLUSTER",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.OLIGO_CONTROL_MUTATION_PER_CLUSTER", "model_path": "awesome.OLIGO_CONTROL_MUTATION_PER_CLUSTER", "is_test": False}
    )

    sales_TDW_OLIGO_CONTROL_MUTATION_TYPES_PER_CLUSTER = PythonOperator(
        task_id="sales_TDW.OLIGO_CONTROL_MUTATION_TYPES_PER_CLUSTER",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.OLIGO_CONTROL_MUTATION_TYPES_PER_CLUSTER", "model_path": "awesome.OLIGO_CONTROL_MUTATION_TYPES_PER_CLUSTER", "is_test": False}
    )

    sales_TDW_OLIGO_CONTROL_SUMMARY = PythonOperator(
        task_id="sales_TDW.OLIGO_CONTROL_SUMMARY",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.OLIGO_CONTROL_SUMMARY", "model_path": "awesome.OLIGO_CONTROL_SUMMARY", "is_test": False}
    )

    sales_TDW_OLIGO_CONTROL_YIELD = PythonOperator(
        task_id="sales_TDW.OLIGO_CONTROL_YIELD",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.OLIGO_CONTROL_YIELD", "model_path": "awesome.OLIGO_CONTROL_YIELD", "is_test": False}
    )

    sales_TDW_DIM_ACCOUNT = PythonOperator(
        task_id="sales_TDW.DIM_ACCOUNT",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.DIM_ACCOUNT", "model_path": "salesforce.DIM_ACCOUNT", "is_test": False}
    )

    sales_TDW_DIM_OPPORTUNITY = PythonOperator(
        task_id="sales_TDW.DIM_OPPORTUNITY",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.DIM_OPPORTUNITY", "model_path": "salesforce.DIM_OPPORTUNITY", "is_test": False}
    )

    sales_TDW_FACT_SALESORDER_ITEM = PythonOperator(
        task_id="sales_TDW.FACT_SALESORDER_ITEM",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.FACT_SALESORDER_ITEM", "model_path": "salesforce.FACT_SALESORDER_ITEM", "is_test": False}
    )

    sales_TDW_FACT_SALES_INVOICE_LINES = PythonOperator(
        task_id="sales_TDW.FACT_SALES_INVOICE_LINES",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.FACT_SALES_INVOICE_LINES", "model_path": "salesforce.FACT_SALES_INVOICE_LINES", "is_test": False}
    )

    sales_TDW_FACT_SALES_ORDER = PythonOperator(
        task_id="sales_TDW.FACT_SALES_ORDER",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_TDW.FACT_SALES_ORDER", "model_path": "salesforce.FACT_SALES_ORDER", "is_test": False}
    )

    sales_meta_stg_dbt_audit_log = PythonOperator(
        task_id="sales_meta.stg_dbt_audit_log",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_meta.stg_dbt_audit_log", "model_path": "stg_dbt_audit_log", "is_test": False}
    )

    sales_meta_stg_dbt_deployments = PythonOperator(
        task_id="sales_meta.stg_dbt_deployments",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_meta.stg_dbt_deployments", "model_path": "default.stg_dbt_deployments", "is_test": False}
    )

    sales_meta_stg_dbt_model_deployments = PythonOperator(
        task_id="sales_meta.stg_dbt_model_deployments",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_sales_meta.stg_dbt_model_deployments", "model_path": "default.stg_dbt_model_deployments", "is_test": False}
    )

    sales_TDW_OLIGO_CONTROL_SUMMARY >> sales_TDW_OLIGO_CONTROL_DEVICE_READ_BINS
    sales_TDW_OLIGO_CONTROL_SUMMARY >> sales_TDW_OLIGO_CONTROL_MUTATIONS_PER_POSITION
    sales_TDW_OLIGO_CONTROL_SUMMARY >> sales_TDW_OLIGO_CONTROL_MUTATION_PER_CLUSTER
    sales_TDW_OLIGO_CONTROL_YIELD >> sales_TDW_OLIGO_CONTROL_SUMMARY
    sales_TDW_OLIGO_CONTROL_BOIA >> sales_TDW_OLIGO_CONTROL_SUMMARY
    sales_TDW_DIM_ACCOUNT >> sales_TDW_FACT_SALESORDER_ITEM
    sales_meta_stg_dbt_audit_log >> sales_meta_stg_dbt_deployments
    sales_meta_stg_dbt_audit_log >> sales_meta_stg_dbt_model_deployments