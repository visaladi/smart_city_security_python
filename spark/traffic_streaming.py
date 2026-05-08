# spark/traffic_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp,
    window, avg, count, when, lit
)

KAFKA_BOOTSTRAP = "localhost:29092"
TRAFFIC_TOPIC = "traffic_raw"

DB_URL = "jdbc:postgresql://localhost:5432/smart_city_traffic"
DB_USER = "postgres"
DB_PASSWORD = "0956"
DB_DRIVER = "org.postgresql.Driver"

spark = (
    SparkSession.builder
        .appName("SmartCityTrafficStreaming_Kafka")
        .master("local[*]")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True),
])


# ---------------- POSTGRES WRITERS ----------------

def write_readings_to_postgres(batch_df, batch_id):
    print(f"\n========== RAW BATCH {batch_id} ==========")
    batch_df.show(10, truncate=False)
    print(f"Raw rows: {batch_df.count()}")

    (
        batch_df
        .selectExpr(
            "sensor_id",
            "unix_timestamp(event_time) as timestamp",
            "cast(vehicle_count as int) as vehicle_count",
            "avg_speed"
        )
        .write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "traffic_readings")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", DB_DRIVER)
        .mode("append")
        .save()
    )

def write_aggregates_to_postgres(batch_df, batch_id):
    print(f"\n========== AGGREGATE BATCH {batch_id} ==========")
    batch_df.show(10, truncate=False)
    print(f"Aggregate rows: {batch_df.count()}")

    (
        batch_df
        .write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "traffic_aggregates")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", DB_DRIVER)
        .mode("append")
        .save()
    )


def write_alerts_to_postgres(batch_df, batch_id):
    print(f"\n========== ALERT BATCH {batch_id} ==========")
    batch_df.show(10, truncate=False)
    print(f"Alert rows: {batch_df.count()}")

    alerts_out = (
        batch_df
        .selectExpr(
            "sensor_id",
            "event_time",
            "avg_speed",
            "cast(vehicle_count as int) as vehicle_count"
        )
        .withColumn("alert_message", lit("Critical traffic: avg_speed below 10 km/h"))
    )

    (
        alerts_out
        .write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "critical_traffic_alerts")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", DB_DRIVER)
        .mode("append")
        .save()
    )


# ---------------- STREAMING PIPELINE ----------------

raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TRAFFIC_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
)

parsed = (
    raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select(
            col("data.sensor_id").alias("sensor_id"),
            to_timestamp(from_unixtime(col("data.timestamp"))).alias("event_time"),
            col("data.vehicle_count").cast("double").alias("vehicle_count"),
            col("data.avg_speed").cast("double").alias("avg_speed"),
        )
        .where(col("event_time").isNotNull())
)

windowed = (
    parsed
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("sensor_id")
        )
        .agg(
            count("*").alias("records"),
            avg("vehicle_count").alias("avg_vehicle_count"),
            avg("avg_speed").alias("avg_speed")
        )
)

congestion = (
    windowed
        .withColumn(
            "congestion_index",
            0.7 * col("avg_vehicle_count") +
            0.3 * when(col("avg_speed") < 30, 30 - col("avg_speed")).otherwise(0)
        )
        .select(
            col("sensor_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("records").cast("int").alias("records"),
            col("avg_vehicle_count"),
            col("avg_speed"),
            col("congestion_index"),
        )
)

alerts = parsed.where(col("avg_speed") < 10)


query_readings = (
    parsed.writeStream
        .foreachBatch(write_readings_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", "file:///C:/tmp/spark_checkpoints/readings")
        .start()
)

query_congestion = (
    congestion.writeStream
        .foreachBatch(write_aggregates_to_postgres)
        .outputMode("update")
        .option("checkpointLocation", "file:///C:/tmp/spark_checkpoints/congestion")
        .start()
)

query_alerts = (
    alerts.writeStream
        .foreachBatch(write_alerts_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", "file:///C:/tmp/spark_checkpoints/alerts")
        .start()
)

spark.streams.awaitAnyTermination()