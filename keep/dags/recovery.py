from datetime import datetime, timedelta
from pathlib import Path
from pprint import pformat
import json
import logging
import os
import sys

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup


# set to use with Bitnami's Airflow image only
AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
BASE_DIR = f"{AIRFLOW_HOME}/dags"

full_path = os.path.dirname(os.path.realpath(__file__))
parent_path = full_path.split(BASE_DIR)[1]

main_dir = str(Path(parent_path).parents[0])
project_name = main_dir.lstrip("/")

PROJECT_NAME = project_name + "_recovery"
DAG_ID = PROJECT_NAME
START_DATE = datetime(2022, 1, 1)
SCHEDULE_INTERVAL = "0 1 * * *"
DAGRUN_TIMEOUT = timedelta(days=1)
HOME_DIR = f"{BASE_DIR}{main_dir}"
WORKING_DIR = f"{HOME_DIR}"

VERSION = ""

with open(f"{HOME_DIR}/VERSION", "r") as version_file:
    VERSION = version_file.read().strip()

PROJECT_TAG = "my-ais-2-0-tracking-dashboard-etl"
PROJECT_DESC = f"MyAIS 2.0 Tracking Dashboard (Insight) {VERSION}"
KERNEL_NAME = f"python-3-12-{PROJECT_TAG}"

sys.path.append(WORKING_DIR)
sys.path.append(f"{WORKING_DIR}/dags")
os.environ["HOME"] = HOME_DIR

os.environ["JUPYTER_CONFIG_DIR"] = "/opt/bitnami/jupyter"

log_formatter = logging.Formatter("%(asctime)s %(levelname)s [airflow-dag] %(message)s")

log_handler = logging.StreamHandler(sys.stdout)
log_handler.setLevel(logging.DEBUG)
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
logger.setLevel(logging.DEBUG)


def load_config(file_name: str):
    env = os.environ.get("ENVIRONMENT", "dev")

    config_path = f"{WORKING_DIR}/config/{env}/{file_name}.{env}.json"

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    return config


@task(task_id="version")
def version():
    logger.info(f"Running Version: {VERSION}")


@task(task_id="debug")
def debug():
    logger.debug(f"Current base dir: {BASE_DIR}")
    logger.debug(f"Current home dir: {HOME_DIR}")
    logger.debug(f"Current working dir: {WORKING_DIR}")

    logger.debug(full_path)
    logger.debug(parent_path)
    logger.debug(main_dir)

    logger.debug(PROJECT_NAME)

    logger.debug(pformat(sys.path))
    logger.debug(pformat(os.environ))


@task(task_id="on_success")
def on_success(context):
    logger.info(f"DAG has succeeded, run_id: {context['run_id']}")


def build_execution_group(
    task_id: str,
    config_file_name: str,
    output_prefix: str,
    output_suffix: str,
):
    with TaskGroup(group_id=task_id) as group:
        config = load_config(file_name=config_file_name)

        app = config["app"]
        app_execution = app["execution"]

        target_notebook_path = app_execution["inputPath"]

        build_execution(
            task_id="execute_etl",
            notebook_file_path=target_notebook_path,
            config=config,
            name="etl",
            output_prefix=output_prefix,
            output_suffix=output_suffix,
        )

    return group


def build_execution(
    task_id: str,
    name: str,
    notebook_file_path: str,
    config: dict,
    output_prefix: str,
    output_suffix: str,
):
    from dags.operator import CustomPapermillOperator

    output_file_name = f"{output_prefix}_{name}_{output_suffix}.ipynb"
    dependencies = config["dependencies"]

    return CustomPapermillOperator(
        task_id=task_id,
        input_nb=os.path.join(WORKING_DIR, "notebooks", notebook_file_path),
        output_nb="/tmp/" + output_file_name,
        parameters={
            "config": config,
            "workingDir": WORKING_DIR,
            "isAirflow": True,
            "isExecuteRecoveryProcess": True,
        },
        logger=logger,
        base_dir=BASE_DIR,
        project_desc=PROJECT_DESC,
        kernel_name=KERNEL_NAME,
        is_kernel_initialized=False,
        dependencies=dependencies,
    )


with DAG(
    dag_id=DAG_ID,
    default_args={
        "owner": "data-driven",
        "depends_on_past": False,
        "retries": 0,
        "on_success_callback": on_success,
    },
    description=f"{PROJECT_DESC} - Recovery",
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    # dagrun_timeout=DAGRUN_TIMEOUT,
    tags=["my-ais-2-0-tracking-dashboard", DAG_ID, PROJECT_TAG, VERSION],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
) as dag:
    version()

    debug()

    build_execution_group(
        task_id="execute",
        config_file_name="config-recovery",
        output_prefix="output",
        output_suffix="{{ts}}",
    )
