from datetime import datetime
from pathlib import Path
import os
import sys
import json

from airflow import DAG
from airflow.utils.task_group import TaskGroup

# Environment setup
AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
ENV = os.environ.get("ENVIRONMENT", "dev")

# Project settings
PROJECT_NAME = "POC_AIS_ETL"
PROJECT_TAG = "my-ais-2-0-tracking-dashboard-etl"
PROJECT_DESC = "MyAIS 2.0 Tracking Dashboard (Insight) 1.0"
KERNEL_NAME = f"python-3-12-{PROJECT_TAG}"

# Paths
WORKING_DIR = os.path.join(AIRFLOW_HOME, "dags")
CONFIG_DIR = os.path.join(AIRFLOW_HOME, "config", ENV)
NOTEBOOK_DIR = os.path.join(WORKING_DIR, "notebooks")

# Append paths to sys.path
sys.path.append(WORKING_DIR)
sys.path.append(os.path.join(AIRFLOW_HOME, "src"))
from operators.custom_papermill_operator import CustomPapermillOperator


# Utility function
def build_execution(task_id, notebook_file_path, config, name, output_prefix, output_suffix):
    output_file_name = f"{output_prefix}_{name}_{output_suffix}.ipynb"
    return CustomPapermillOperator(
        task_id=task_id,
        input_nb=os.path.join(NOTEBOOK_DIR, notebook_file_path),
        output_nb=os.path.join("/tmp", output_file_name),
        parameters={
            "config": config,
            "workingDir": WORKING_DIR,
            "isAirflow": True,
        },
        base_dir=WORKING_DIR,
        project_desc=PROJECT_DESC,
        kernel_name=KERNEL_NAME,
        is_kernel_initialized=False,
        dependencies=config.get("dependencies", []),
    )

def build_execution_group(task_id, config_file_name, output_prefix, output_suffix):
    config_path = os.path.join(CONFIG_DIR, f"{config_file_name}.{ENV}.json")
    with TaskGroup(group_id=task_id) as group:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        app_execution = config.get("app", {}).get("execution", {})
        target_notebook_path = app_execution.get("inputPath", "")

        build_execution(
            task_id="execute_etl",
            notebook_file_path=target_notebook_path,
            config=config,
            name="etl",
            output_prefix=output_prefix,
            output_suffix=output_suffix,
        )

    return group

def on_success(context):
    print(f"Task completed successfully! TASK_ID: {context['run_id']}")

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "on_success_callback": on_success,
}

with DAG(
    dag_id=PROJECT_NAME,
    start_date=datetime(2022, 1, 1),
    schedule="*/5 * * * *",
    default_args=default_args,
    catchup=False,
) as dag:
    build_execution_group(
        task_id="execute",
        config_file_name="config",
        output_prefix="output",
        output_suffix="{{ ts }}",
    )
