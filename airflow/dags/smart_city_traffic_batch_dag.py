from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

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

TS_EXPR = 'to_timestamp("timestamp")'


def run_sql(sql, params=None):
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    cur.close()
    conn.close()


def ensure_report_table():
    run_sql("""
    CREATE TABLE IF NOT EXISTS traffic_readings (
        id SERIAL PRIMARY KEY,
        sensor_id TEXT,
        "timestamp" BIGINT,
        vehicle_count INT,
        avg_speed DOUBLE PRECISION
    );

    CREATE TABLE IF NOT EXISTS traffic_aggregates (
        sensor_id TEXT,
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        records INT,
        avg_vehicle_count DOUBLE PRECISION,
        avg_speed DOUBLE PRECISION,
        congestion_index DOUBLE PRECISION
    );

    CREATE TABLE IF NOT EXISTS critical_traffic_alerts (
        id SERIAL PRIMARY KEY,
        sensor_id TEXT,
        event_time TIMESTAMP,
        avg_speed DOUBLE PRECISION,
        vehicle_count INT,
        alert_message TEXT
    );

    CREATE TABLE IF NOT EXISTS daily_peak_hour_report (
        report_date DATE NOT NULL,
        sensor_id TEXT NOT NULL,
        peak_hour TIMESTAMP NOT NULL,
        total_vehicles BIGINT NOT NULL,
        needs_police BOOLEAN NOT NULL,
        PRIMARY KEY (report_date, sensor_id)
    );

    CREATE TABLE IF NOT EXISTS last24_peak_hour_report (
        generated_at TIMESTAMP NOT NULL,
        sensor_id TEXT NOT NULL,
        peak_hour TIMESTAMP NOT NULL,
        total_vehicles BIGINT NOT NULL,
        needs_police BOOLEAN NOT NULL,
        PRIMARY KEY (generated_at, sensor_id)
    );
    """)

def generate_last24_peak_report():
    run_sql("""
    DELETE FROM last24_peak_hour_report;

    WITH latest AS (
        SELECT MAX(to_timestamp("timestamp")) AS max_time
        FROM traffic_readings
    ),
    hourly AS (
        SELECT
            sensor_id,
            date_trunc('hour', to_timestamp("timestamp")) AS peak_hour,
            SUM(vehicle_count) AS total_vehicles
        FROM traffic_readings, latest
        WHERE to_timestamp("timestamp") >= latest.max_time - interval '24 hours'
        GROUP BY sensor_id, date_trunc('hour', to_timestamp("timestamp"))
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY sensor_id
                ORDER BY total_vehicles DESC
            ) AS rn
        FROM hourly
    ),
    peak_per_sensor AS (
        SELECT sensor_id, peak_hour, total_vehicles
        FROM ranked
        WHERE rn = 1
    ),
    worst AS (
        SELECT sensor_id
        FROM peak_per_sensor
        ORDER BY total_vehicles DESC
        LIMIT 1
    )
    INSERT INTO last24_peak_hour_report
    (generated_at, sensor_id, peak_hour, total_vehicles, needs_police)
    SELECT
        NOW(),
        p.sensor_id,
        p.peak_hour,
        p.total_vehicles,
        p.sensor_id = w.sensor_id
    FROM peak_per_sensor p
    CROSS JOIN worst w;
    """)


def generate_daily_peak_report(**context):
    run_sql(f"""
    DELETE FROM daily_peak_hour_report
    WHERE report_date = (
        SELECT MAX({TS_EXPR})::date FROM traffic_readings
    );

    WITH latest_day AS (
        SELECT MAX({TS_EXPR})::date AS report_date
        FROM traffic_readings
    ),
    hourly AS (
        SELECT
            sensor_id,
            date_trunc('hour', {TS_EXPR}) AS peak_hour,
            SUM(vehicle_count) AS total_vehicles
        FROM traffic_readings, latest_day
        WHERE {TS_EXPR}::date = latest_day.report_date
        GROUP BY sensor_id, date_trunc('hour', {TS_EXPR})
    ),
    ranked AS (
        SELECT
            sensor_id,
            peak_hour,
            total_vehicles,
            ROW_NUMBER() OVER (
                PARTITION BY sensor_id
                ORDER BY total_vehicles DESC
            ) AS rn
        FROM hourly
    ),
    peak_per_sensor AS (
        SELECT sensor_id, peak_hour, total_vehicles
        FROM ranked
        WHERE rn = 1
    ),
    worst AS (
        SELECT sensor_id
        FROM peak_per_sensor
        ORDER BY total_vehicles DESC
        LIMIT 1
    )
    INSERT INTO daily_peak_hour_report
        (report_date, sensor_id, peak_hour, total_vehicles, needs_police)
    SELECT
        latest_day.report_date,
        p.sensor_id,
        p.peak_hour,
        p.total_vehicles,
        p.sensor_id = w.sensor_id AS needs_police
    FROM peak_per_sensor p
    CROSS JOIN worst w
    CROSS JOIN latest_day;
    """)


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

    t1 >> t2 >> t3