import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

conn = psycopg2.connect(
    host="localhost",
    dbname="smart_city_traffic",
    user="postgres",
    password="0956",
    port=5432,
)

query = """
SELECT
  date_trunc('hour', window_start) AS hour,
  sensor_id,
  SUM(avg_vehicle_count) AS total_vehicle_count
FROM traffic_aggregates
WHERE date(window_start) = CURRENT_DATE
GROUP BY hour, sensor_id
ORDER BY hour, sensor_id;
"""

df = pd.read_sql(query, conn)
conn.close()

pivot = df.pivot(index="hour", columns="sensor_id", values="total_vehicle_count")

pivot.plot()
plt.xlabel("Time of Day")
plt.ylabel("Traffic Volume (vehicles/hour)")
plt.title("Traffic Volume vs Time of Day per Junction")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
