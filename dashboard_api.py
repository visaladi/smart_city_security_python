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

app = FastAPI(title="Smart City Traffic Dashboard API")

# Allow browser fetch from localhost
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

@app.get("/api/raw/latest")
def get_latest_raw(limit: int = 50):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT sensor_id, to_timestamp(timestamp) AS event_time,
               vehicle_count, avg_speed
        FROM traffic_readings
        ORDER BY timestamp DESC
        LIMIT %s;
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@app.get("/api/aggregates/latest")
def get_latest_aggregates():
    """
    Latest 5-min window per sensor from traffic_aggregates
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
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
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# Serve static static
app.mount(
    "/", StaticFiles(directory="static", html=True), name="static"
)
