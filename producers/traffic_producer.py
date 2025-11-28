import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

SENSORS = ["J1", "J2", "J3", "J4"]

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",  # your Kafka broker
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_record(sensor_id: str) -> dict:
    # normal traffic
    vehicle_count = random.randint(5, 30)
    avg_speed = random.uniform(20, 50)

    # sometimes simulate congestion (avg_speed < 10)
    if random.random() < 0.10:
        vehicle_count = random.randint(40, 80)
        avg_speed = random.uniform(3, 9)

    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.utcnow().isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": round(avg_speed, 2),
    }

def main():
    print("Starting traffic producer...")
    while True:
        for sensor in SENSORS:
            record = generate_record(sensor)
            producer.send("traffic_raw", record)
            print("Sent:", record)
        producer.flush()
        time.sleep(1)

if __name__ == "__main__":
    main()
