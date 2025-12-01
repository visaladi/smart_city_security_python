#spark/traffic_batch_postgres.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_timestamp,
    window,
    avg,
    count,
    when
)

# -------------------------------------------------------------------
# Spark Session (batch mode, no streaming, no checkpointing)
# -------------------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("SmartCityTrafficBatchAnalytics")
        .master("local[*]")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------------------
# JDBC config (adjust if needed)
# -------------------------------------------------------------------
jdbc_url = "jdbc:postgresql://localhost:5432/smart_city_traffic"
jdbc_props = {
    "user": "postgres",
    "password": "0956",
    "driver": "org.postgresql.Driver"
}

# -------------------------------------------------------------------
# 1. Read raw traffic data from Postgres
#    Assume Spring Boot table name: traffic_readings
#    and columns: id, sensor_id, timestamp, vehicle_count, avg_speed
# -------------------------------------------------------------------

print("=== Reading raw traffic data from PostgreSQL ===")

traffic_df = (
    spark.read
         .jdbc(jdbc_url, "traffic_readings", properties=jdbc_props)
)

# If your timestamp column is BIGINT epoch seconds, convert it.
# If it's already TIMESTAMP, comment this and just rename.
if "timestamp" in traffic_df.columns:
    traffic_df = traffic_df.withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("timestamp")))
    )
else:
    # fallback in case your column is named differently
    raise Exception("Expected column 'timestamp' in traffic_readings table")

traffic_df = (
    traffic_df
        .select(
            col("sensor_id"),
            col("event_time"),
            col("vehicle_count").cast("double"),
            col("avg_speed").cast("double")
        )
        .where(col("event_time").isNotNull())
)

print("=== Sample raw data ===")
traffic_df.show(10, truncate=False)

# -------------------------------------------------------------------
# 2. Compute 5-minute congestion window stats
# -------------------------------------------------------------------

windowed_df = (
    traffic_df
        .groupBy(
            window(col("event_time"), "5 minutes").alias("time_window"),
            col("sensor_id")
        )
        .agg(
            count("*").alias("records"),
            avg("vehicle_count").alias("avg_vehicle_count"),
            avg("avg_speed").alias("avg_speed")
        )
)

# Simple congestion index:
#   congestion_index = vehicle_count weight * avg_vehicle_count
#                      + speed penalty * max(0, (30 - avg_speed))
# Tune weights as you like.
congestion_df = (
    windowed_df
        .withColumn(
            "congestion_index",
            0.7 * col("avg_vehicle_count") +
            0.3 * when(col("avg_speed") < 30, 30 - col("avg_speed")).otherwise(0)
        )
        .select(
            col("sensor_id"),
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("records"),
            col("avg_vehicle_count"),
            col("avg_speed"),
            col("congestion_index")
        )
        .orderBy("window_start", "sensor_id")
)

print("=== 5-minute congestion metrics ===")
congestion_df.show(50, truncate=False)

# -------------------------------------------------------------------
# 3. (Optional) Write results back to PostgreSQL
#    New table: traffic_aggregates
# -------------------------------------------------------------------

(
    congestion_df
        .write
        .mode("overwrite")  # or "append" if you want to keep history
        .jdbc(jdbc_url, "traffic_aggregates", properties=jdbc_props)
)

print("=== Written congestion aggregates to table 'traffic_aggregates' ===")

spark.stop()
