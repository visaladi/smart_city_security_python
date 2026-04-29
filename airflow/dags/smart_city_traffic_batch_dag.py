from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import pandas as pd

default_args = {
    "owner": "visal",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PG = {
    "host": "postgres-traffic",
    "port": 5432,
    "database": "smart_city_traffic",
    "user": "postgres",
    "password": "0956",
}

# If your traffic_readings.timestamp is milliseconds, change TS_EXPR to:
# TS_EXPR = 'to_timestamp("timestamp" / 1000.0)'
TS_EXPR = 'to_timestamp("timestamp")'


def ensure_report_table():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_peak_hour_report (
      report_date DATE NOT NULL,
      sensor_id TEXT NOT NULL,
      peak_hour TIMESTAMP NOT NULL,
      total_vehicles BIGINT NOT NULL,
      needs_police BOOLEAN NOT NULL,
      PRIMARY KEY (report_date, sensor_id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS last24_peak_hour_report (
      generated_at TIMESTAMP NOT NULL,
      sensor_id TEXT NOT NULL,
      peak_hour TIMESTAMP NOT NULL,
      total_vehicles BIGINT NOT NULL,
      needs_police BOOLEAN NOT NULL,
      PRIMARY KEY (generated_at, sensor_id)
    );
    """)

    conn.commit()
    cur.close()
    conn.close()


def generate_last24_peak_report():
    conn = psycopg2.connect(**PG)

    df = pd.read_sql(
        f"""
        SELECT
          sensor_id,
          date_trunc('hour', {TS_EXPR}) AS hour,
          SUM(vehicle_count) AS total_vehicles
        FROM traffic_readings
        WHERE {TS_EXPR} >= NOW() - interval '24 hours'
        GROUP BY sensor_id, hour
        ORDER BY sensor_id, hour;
        """,
        conn,
    )

    if df.empty:
        conn.close()
        print("No data in last 24 hours")
        return

    peak = (
        df.sort_values(["sensor_id", "total_vehicles"], ascending=[True, False])
          .groupby("sensor_id")
          .head(1)
          .reset_index(drop=True)
    )

    worst_sensor = peak.sort_values("total_vehicles", ascending=False).iloc[0]["sensor_id"]
    peak["needs_police"] = peak["sensor_id"].eq(worst_sensor)

    generated_at = pd.Timestamp.utcnow()

    cur = conn.cursor()
    for _, row in peak.iterrows():
        cur.execute(
            """
            INSERT INTO last24_peak_hour_report (generated_at, sensor_id, peak_hour, total_vehicles, needs_police)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (generated_at, sensor_id)
            DO UPDATE SET
              peak_hour = EXCLUDED.peak_hour,
              total_vehicles = EXCLUDED.total_vehicles,
              needs_police = EXCLUDED.needs_police;
            """,
            (generated_at, row["sensor_id"], row["hour"], int(row["total_vehicles"]), bool(row["needs_police"]))
        )

    conn.commit()
    cur.close()
    conn.close()


def generate_daily_peak_report(**context):
    report_date = context["ds"]  # YYYY-MM-DD

    conn = psycopg2.connect(**PG)

    df = pd.read_sql(
        f"""
        SELECT
          sensor_id,
          date_trunc('hour', {TS_EXPR}) AS hour,
          SUM(vehicle_count) AS total_vehicles
        FROM traffic_readings
        WHERE {TS_EXPR}::date = %s::date
        GROUP BY sensor_id, hour
        ORDER BY sensor_id, hour;
        """,
        conn,
        params=(report_date,),
    )

    if df.empty:
        conn.close()
        print(f"No data in traffic_readings for {report_date}")
        return

    peak = (
        df.sort_values(["sensor_id", "total_vehicles"], ascending=[True, False])
          .groupby("sensor_id")
          .head(1)
          .reset_index(drop=True)
    )

    worst_sensor = peak.sort_values("total_vehicles", ascending=False).iloc[0]["sensor_id"]
    peak["needs_police"] = peak["sensor_id"].eq(worst_sensor)

    cur = conn.cursor()
    for _, row in peak.iterrows():
        cur.execute(
            """
            INSERT INTO daily_peak_hour_report (report_date, sensor_id, peak_hour, total_vehicles, needs_police)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (report_date, sensor_id)
            DO UPDATE SET
              peak_hour = EXCLUDED.peak_hour,
              total_vehicles = EXCLUDED.total_vehicles,
              needs_police = EXCLUDED.needs_police;
            """,
            (report_date, row["sensor_id"], row["hour"], int(row["total_vehicles"]), bool(row["needs_police"]))
        )

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="smart_city_traffic_batch_dag",
    default_args=default_args,
    description="Nightly job: last 24h peak hour + daily peak hour",
    schedule="0 1 * * *",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["smart_city", "traffic", "batch"],
) as dag:

    t1 = PythonOperator(
        task_id="ensure_report_table",
        python_callable=ensure_report_table,
    )

    t2 = PythonOperator(
        task_id="generate_last24_peak_report",
        python_callable=generate_last24_peak_report,
    )

    t3 = PythonOperator(
        task_id="generate_daily_peak_report",
        python_callable=generate_daily_peak_report,
    )

    # Order: last24 first, daily last
    t1 >> t2 >> t3
