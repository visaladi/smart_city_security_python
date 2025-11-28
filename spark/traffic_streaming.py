import os

# ============================================================
# DISABLE HADOOP ON WINDOWS (NO WINUTILS NEEDED)
# ============================================================
os.environ["HADOOP_HOME"] = ""
os.environ["hadoop.home.dir"] = ""
#os.environ["SPARK_HOME"] = ""
os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col, window, avg, sum as _sum

# ============================================================
# DATABASE CONFIG
# ============================================================
DB_URL = "jdbc:postgresql://localhost:5432/smart_city_traffic"
DB_USER = "postgres"
DB_PASSWORD = "0956"


# ============================================================
# SPARK SESSION (NO HADOOP MODE)
# ============================================================
spark = (
    SparkSession.builder
    .appName("SmartCityTrafficStreaming")
    .master("local[*]")  # IMPORTANT: run locally, no Hadoop cluster
    # FORCE LOCAL FILESYSTEM INSTEAD OF HADOOP FS
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem")

    # JDBC driver
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# KAFKA MESSAGE SCHEMA
# ============================================================
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True),
])


# ============================================================
# READ FROM KAFKA
# ============================================================
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "traffic_raw")
    .option("startingOffsets", "latest")
    .load()
)

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = (
    json_df
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)


# ============================================================
# 5-MINUTE WINDOW AGGREGATIONS
# ============================================================
windowed_df = (
    parsed_df
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor_id")
    )
    .agg(
        _sum("vehicle_count").alias("total_vehicles"),
        avg("avg_speed").alias("mean_speed")
    )
    .withColumn(
        "congestion_index",
        col("total_vehicles") / col("mean_speed")
    )
)


# Print the windowed results to console
windowed_query = (
    windowed_df
    .writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", 50)
    .option("checkpointLocation", "chk/traffic_windows")
    .start()
)


# ============================================================
# ALERTS (avg_speed < 10)
# ============================================================
alerts_df = parsed_df.filter(col("avg_speed") < 10)

alerts_query = (
    alerts_df
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "chk/traffic_alerts")
    .start()
)


# ============================================================
# WRITE TO POSTGRES
# ============================================================
def write_to_postgres(batch_df, batch_id):
    (
        batch_df
        .select("sensor_id", "event_time", "vehicle_count", "avg_speed")
        .write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "traffic_events")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


events_query = (
    parsed_df
    .writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "chk/traffic_events")
    .start()
)


# ============================================================
# START STREAMING
# ============================================================
spark.streams.awaitAnyTermination()
