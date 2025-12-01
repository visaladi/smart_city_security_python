#spark/traffic_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    to_timestamp,
    window,
    avg,
    count,
    when
)

KAFKA_BOOTSTRAP = "localhost:29092"   # match your docker-compose (kafka-smartcity)
TRAFFIC_TOPIC = "traffic-data"        # match Spring Boot producer

spark = (
    SparkSession.builder
        .appName("SmartCityTrafficStreaming_Kafka")
        .master("local[*]")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema of your JSON messages from Spring Boot producer
schema = StructType([
    StructField("sensorId", StringType(), True),
    StructField("timestamp", LongType(), True),       # epoch seconds
    StructField("vehicleCount", IntegerType(), True),
    StructField("avgSpeed", DoubleType(), True)
])

# 1. Read from Kafka as streaming source
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
            col("data.sensorId").alias("sensor_id"),
            to_timestamp(from_unixtime(col("data.timestamp"))).alias("event_time"),
            col("data.vehicleCount").cast("double").alias("vehicle_count"),
            col("data.avgSpeed").cast("double").alias("avg_speed")
        )
        .where(col("event_time").isNotNull())
)

# 2. Windowed congestion metrics
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
            col("congestion_index")
        )
)

# 3. Critical alerts (speed < 10)
alerts = parsed.where(col("avg_speed") < 10)

# Sink 1: print congestion metrics
query_congestion = (
    congestion.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "file:///C:/tmp/spark_checkpoints/congestion")  # adjust
        .start()
)

# Sink 2: print alerts
query_alerts = (
    alerts.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "file:///C:/tmp/spark_checkpoints/alerts")      # adjust
        .start()
)

spark.streams.awaitAnyTermination()
