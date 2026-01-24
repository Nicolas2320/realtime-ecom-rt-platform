import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, coalesce, to_date, hour, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

TOPIC = os.getenv("TOPIC", "ecom.events.raw.v1")
BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-0:9092")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio12345")

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "s3a://lake/bronze/ecom_events/v1/")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://lake/_checkpoints/bronze/ecom_events_v1/")

STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

TRIGGER = os.getenv("TRIGGER", "1 minute")

# Spark schema for typed Bronze
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_version", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("source", StringType(), True),
])

def build_spark():
    spark = (
        SparkSession.builder
        .appName("bronze-writer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark

def main():
    spark = build_spark()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Keep Kafka metadata & raw JSON
    base = raw.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS raw_json",
        "topic AS kafka_topic",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
        "timestamp AS kafka_timestamp"
    )

    parsed = base.withColumn("json", from_json(col("raw_json"), event_schema))

    flat = parsed.select(
        "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "raw_json",
        col("json.*")
    )

    # Robust timestamp parse (because generator uses ...SSSZ)
    ingest_ts = coalesce(
        to_timestamp(col("ingest_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        to_timestamp(col("ingest_time"))
    )

    out = (
        flat
        .withColumn("ingest_time_ts", ingest_ts)
        .withColumn("ingest_date", to_date(col("ingest_time_ts")))
        .withColumn("ingest_hour", hour(col("ingest_time_ts")))
        .withColumn("processing_time", current_timestamp())
    )

    query = (
        out.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingest_date", "ingest_hour")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()