from datetime import datetime, timedelta
from pathlib import Path
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Set path
AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
sys.path.append(os.path.join(AIRFLOW_HOME, "src"))  # ✅ เพิ่ม src เข้า path

# Import ฟังก์ชัน
from app.run import say_hello

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="my_project",
    start_date=datetime(2022, 1, 1),
    schedule="*/10 * * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    greet = PythonOperator(
        task_id="say_hello",
        python_callable=lambda: say_hello("Wanchai"),
    )
