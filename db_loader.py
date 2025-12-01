#db_loader.py
import json
import psycopg2
from kafka import KafkaConsumer

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="smart_city_traffic",
    user="postgres",
    password="0956",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Create table if not exists
cur.execute("""
CREATE TABLE IF NOT EXISTS traffic_readings (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(10),
    timestamp BIGINT,
    vehicle_count INT,
    avg_speed DOUBLE PRECISION
);
""")
conn.commit()

# Kafka consumer
consumer = KafkaConsumer(
    "traffic_raw",
    bootstrap_servers="localhost:29092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest"
)

print("Listening to Kafka...")

for msg in consumer:
    data = msg.value
    print("Received:", data)

    cur.execute("""
        INSERT INTO traffic_readings (sensor_id, timestamp, vehicle_count, avg_speed)
        VALUES (%s, %s, %s, %s)
    """, (
        data["sensor_id"],
        data["timestamp"],
        data["vehicle_count"],
        data["avg_speed"]
    ))

    conn.commit()
