from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "visal",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ⚠ Adjust this to your actual project path
PROJECT_DIR = r"C:\Users\visal\Desktop\uni sem 8\bigdata\final\smart_city_security"
SPARK_SUBMIT = r"C:\spark\bin\spark-submit.cmd"

with DAG(
    dag_id="smart_city_traffic_batch_dag",
    default_args=default_args,
    description="Nightly Spark batch job for Smart City Traffic",
    schedule_interval="0 1 * * *",  # every day at 01:00
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["smart_city", "traffic", "spark"],
) as dag:

    run_spark_batch = BashOperator(
        task_id="run_spark_batch",
        bash_command=fr'"{SPARK_SUBMIT}" --packages org.postgresql:postgresql:42.7.1 "{PROJECT_DIR}\spark\traffic_batch_postgres.py"',
    )

    run_spark_batch
