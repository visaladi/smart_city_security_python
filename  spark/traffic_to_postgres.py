# spark/traffic_to_postgres.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType,
    IntegerType, DoubleType
)

# 1) Create Spark Session with Postgres driver
spark = (
    SparkSession.builder
        .appName("SmartCityTrafficToPostgres")
        # If you run with spark-submit and --packages, you can remove this line
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

# 2) Read from Kafka topic
kafka_bootstrap_servers = "localhost:29092"   # match your docker-compose
kafka_topic = "traffic-events"               # adjust to your actual topic name

raw_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
)

# Kafka 'value' is bytes -> cast to string
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

# 3) Define schema matching your JSON
traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True),
])

parsed_df = (
    json_df
        .select(from_json(col("json_str"), traffic_schema).alias("data"))
        .select("data.*")  # flatten struct -> columns
)

# 4) foreachBatch function to write to Postgres
def write_to_postgres(batch_df, batch_id):
    print(f"Writing micro-batch {batch_id} to Postgres ...")

    url = "jdbc:postgresql://localhost:5432/smart_city_traffic"
    table = "traffic_events"
    properties = {
        "user": "postgres",
        "password": "0956",
        "driver": "org.postgresql.Driver"
    }

    # Filter out obviously bad records (optional but nice)
    cleaned_df = batch_df.dropna(subset=["sensor_id", "event_time"])

    # Append to traffic_events table
    cleaned_df.write.jdbc(
        url=url,
        table=table,
        mode="append",
        properties=properties
    )

# 5) Start the streaming query
query = (
    parsed_df.writeStream
        .outputMode("append")   # we're always appending new events
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", "./checkpoint/traffic_to_postgres")  # local checkpoint dir
        .start()
)

query.awaitTermination()
