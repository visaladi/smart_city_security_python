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
    cur = conn.cursor()

    cur.execute("SELECT MAX(generated_at) FROM last24_peak_hour_report")
    latest = cur.fetchone()[0]
    if latest is None:
        cur.close(); conn.close()
        return []

    cur.execute("""
        SELECT sensor_id, peak_hour, total_vehicles, needs_police
        FROM last24_peak_hour_report
        WHERE generated_at = %s
        ORDER BY total_vehicles DESC;
    """, (latest,))

    rows = cur.fetchall()
    cur.close(); conn.close()

    return [
        {"sensor_id": r[0], "peak_hour": str(r[1]), "total_vehicles": int(r[2]), "needs_police": bool(r[3])}
        for r in rows
    ]



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


# ---------- STATIC FRONTEND ----------

# Make sure you have: smart_city_security/static/index.html
app.mount("/", StaticFiles(directory="static", html=True), name="static")
