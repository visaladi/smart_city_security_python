from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp,
    window, avg, count, when
)

KAFKA_BOOTSTRAP = "localhost:29092"
TRAFFIC_TOPIC = "traffic_raw"   # same as your producer

spark = (
    SparkSession.builder
        .appName("SmartCityTrafficStreaming_Kafka")
        .master("local[*]")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Match your producer schema
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", LongType(), True),       # epoch seconds
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True),
])

# 1) Read Kafka stream
raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TRAFFIC_TOPIC)
        .option("startingOffsets", "latest")
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

# 2) 5-minute windowed congestion metrics (streaming)
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
            col("records"),
            col("avg_vehicle_count"),
            col("avg_speed"),
            col("congestion_index"),
        )
)

# 3) Critical alerts (avg_speed < 10)
alerts = parsed.where(col("avg_speed") < 10)

# Sink 1: print congestion metrics
query_congestion = (
    congestion.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "file:///C:/tmp/spark_checkpoints/congestion")
        .start()
)

# Sink 2: print alerts
query_alerts = (
    alerts.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "file:///C:/tmp/spark_checkpoints/alerts")
        .start()
)

spark.streams.awaitAnyTermination()
