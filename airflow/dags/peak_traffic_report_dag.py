from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import os

DEFAULT_ARGS = {
    "owner": "smartcity",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# CHANGE THIS to your real absolute path
REPORT_DIR = r"C:\Users\visal\Desktop\uni sem 8\bigdata\smart-city-traffic\reports"

def generate_peak_traffic_report(**context):
    # Airflow passes execution date as string "YYYY-MM-DD" in {{ ds }}
    execution_date_str = context["ds"]     # e.g. "2025-11-27"
    exec_date = execution_date_str  # let Postgres cast to date

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="smart_city_traffic",
        user="postgres",
        password="0956",
    )
    cursor = conn.cursor()

    query = """
        SELECT
            sensor_id,
            date_trunc('hour', event_time) AS hour,
            SUM(vehicle_count) AS total_vehicles
        FROM traffic_events
        WHERE event_time::date = %s
        GROUP BY sensor_id, hour
        ORDER BY sensor_id, hour;
    """
    cursor.execute(query, (exec_date,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        print("No data for date", exec_date)
        return

    df = pd.DataFrame(rows, columns=["sensor_id", "hour", "total_vehicles"])

    # Per junction, pick the hour with highest vehicle count
    peak_df = (
        df.sort_values(["sensor_id", "total_vehicles"], ascending=[True, False])
          .groupby("sensor_id")
          .head(1)
          .reset_index(drop=True)
    )

    # Mark which junction should get traffic police (highest volume overall)
    worst_row = peak_df.sort_values("total_vehicles", ascending=False).iloc[0]
    peak_df["needs_police"] = peak_df["sensor_id"] == worst_row["sensor_id"]

    os.makedirs(REPORT_DIR, exist_ok=True)
    out_path = os.path.join(REPORT_DIR, f"peak_traffic_{exec_date}.csv")
    peak_df.to_csv(out_path, index=False)
    print("Saved report to", out_path)

with DAG(
    dag_id="peak_traffic_report",
    default_args=DEFAULT_ARGS,
    description="Nightly peak traffic hour report per junction",
    schedule_interval="0 23 * * *",  # every day at 23:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    report_task = PythonOperator(
        task_id="generate_peak_traffic_report",
        python_callable=generate_peak_traffic_report,
        provide_context=True,
    )
