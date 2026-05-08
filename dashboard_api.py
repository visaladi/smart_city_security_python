# dashboard_api.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "dbname": "smart_city_traffic",
    "user": "postgres",
    "password": "0956",
    "host": "localhost",
    "port": "5432",
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

app = FastAPI(title="Smart City Traffic Dashboard API")

# CORS (so browser JS can call the API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- API ENDPOINTS ----------

@app.get("/api/aggregates/latest")
def get_latest_aggregates():
    """
    Latest 5-minute congestion window per sensor from traffic_aggregates.
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT DISTINCT ON (sensor_id)
            sensor_id,
            window_start,
            window_end,
            records,
            avg_vehicle_count,
            avg_speed,
            congestion_index
        FROM traffic_aggregates
        ORDER BY sensor_id, window_end DESC;
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@app.get("/api/peak/last24")
def last24_peak():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
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

    conn.commit()

    cur.execute("""
        SELECT sensor_id, peak_hour, total_vehicles, needs_police
        FROM last24_peak_hour_report
        WHERE generated_at = (
            SELECT MAX(generated_at) FROM last24_peak_hour_report
        )
        ORDER BY total_vehicles DESC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@app.get("/api/police/intervention")
def police_intervention():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT sensor_id, peak_hour, total_vehicles, needs_police
        FROM last24_peak_hour_report
        WHERE generated_at = (
            SELECT MAX(generated_at) FROM last24_peak_hour_report
        )
        ORDER BY needs_police DESC, total_vehicles DESC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows



@app.get("/api/raw/latest")
def get_latest_raw(limit: int = 50):
    """
    Latest raw readings from traffic_readings (joined with converted timestamp).
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT
            sensor_id,
            to_timestamp(timestamp) AS event_time,
            vehicle_count,
            avg_speed
        FROM traffic_readings
        ORDER BY timestamp DESC
        LIMIT %s;
        """,
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@app.get("/api/police/intervention")
def police_intervention():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT sensor_id, peak_hour, total_vehicles, needs_police
        FROM last24_peak_hour_report
        WHERE generated_at = (
            SELECT MAX(generated_at) FROM last24_peak_hour_report
        )
        ORDER BY needs_police DESC, total_vehicles DESC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# ---------- STATIC FRONTEND ----------

# Make sure you have: smart_city_security/static/index.html
app.mount("/", StaticFiles(directory="static", html=True), name="static")
