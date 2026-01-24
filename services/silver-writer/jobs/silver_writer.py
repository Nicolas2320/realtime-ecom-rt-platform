import os, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, coalesce, to_date, hour, current_timestamp,
    lit, when, array, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, DateType
)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio12345")

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lake/bronze/ecom_events/v1/")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lake/silver/ecom_events/v1/")
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", "s3a://lake/quarantine/ecom_events/v1/")

SILVER_CHECKPOINT = os.getenv("SILVER_CHECKPOINT", "s3a://lake/_checkpoints/silver/ecom_events_v1/")
QUARANTINE_CHECKPOINT = os.getenv("QUARANTINE_CHECKPOINT", "s3a://lake/_checkpoints/quarantine/ecom_events_v1/")

EVENT_WATERMARK = os.getenv("EVENT_WATERMARK", "1 hour")
TRIGGER = os.getenv("TRIGGER", "1 minute")
MAX_FILES_PER_TRIGGER = os.getenv("MAX_FILES_PER_TRIGGER", "50")

# Spark schema for typed Silver:
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
    StructField("raw_json", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_key", StringType(), True),
    StructField("ingest_date", DateType(), True),
    StructField("ingest_hour", IntegerType(), True),

])

def s3a_path_exists(spark: SparkSession, uri: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(uri), hconf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(uri))

def wait_for_path(spark: SparkSession, uri: str) -> None:
    poll = int(os.getenv("WAIT_FOR_BRONZE_POLL_SECONDS", "5"))
    timeout = int(os.getenv("WAIT_FOR_BRONZE_SECONDS", "300"))  # 0 = wait forever

    start = time.time()
    while True:
        if s3a_path_exists(spark, uri):
            print(f"[INFO] Found input path: {uri}")
            return

        elapsed = int(time.time() - start)
        print(f"[INFO] Input path not found yet: {uri} | waiting... ({elapsed}s)")
        time.sleep(poll)

        if timeout > 0 and elapsed >= timeout:
            raise RuntimeError(f"[ERROR] Bronze path still missing after {timeout}s: {uri}")

def build_spark():
    spark = (
        SparkSession.builder
        .appName("silver-writer")
        .config("spark.sql.session.timeZone", "UTC")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark

def main():
    spark = build_spark()

    wait_for_path(spark, BRONZE_PATH)

    bronze = (
        spark.readStream.format("parquet")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(event_schema)
        .option("basePath", BRONZE_PATH)
        .load(BRONZE_PATH)
    )

    # Robust timestamp parse (because generator uses ...SSSZ)
    event_ts = coalesce(
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        to_timestamp(col("event_time"))
    )
    ingest_ts = coalesce(
        to_timestamp(col("ingest_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        to_timestamp(col("ingest_time"))
    )

    df = (
        bronze  
        .withColumn("event_time_ts", event_ts)
        .withColumn("ingest_time_ts", ingest_ts)
        .withColumn("processing_time", current_timestamp())
    )

    # Structural required
    r_event_id = col("event_id").isNotNull()
    r_event_type = col("event_type").isin("click", "add_to_cart", "checkout", "purchase")
    r_user = col("user_id").isNotNull()
    r_session = col("session_id").isNotNull()
    r_ingest_ts = col("ingest_time_ts").isNotNull()
    r_event_ts = col("event_time_ts").isNotNull()

    # click/add_to_cart require product_id
    b_product_for_click_cart = when(
        col("event_type").isin("click", "add_to_cart"),
        col("product_id").isNotNull()
    ).otherwise(lit(True))

    # add_to_cart requires quantity>=1, price>=0, currency not null
    b_cart_fields = when(
        col("event_type") == lit("add_to_cart"),
        (col("quantity").isNotNull()) & (col("quantity") >= 1) &
        (col("price").isNotNull()) & (col("price") >= 0) &
        (col("currency").isNotNull())
    ).otherwise(lit(True))

    # purchase requires order_id, quantity>=1, price>=0, currency not null
    b_purchase_fields = when(
        col("event_type") == lit("purchase"),
        (col("order_id").isNotNull()) &
        (col("quantity").isNotNull()) & (col("quantity") >= 1) &
        (col("price").isNotNull()) & (col("price") >= 0) &
        (col("currency").isNotNull())
    ).otherwise(lit(True))

    # collect error reasons (simple + readable)
    reasons = array(
        when(~r_event_id, lit("missing_event_id")),
        when(~r_event_type, lit("invalid_event_type")),
        when(~r_user, lit("missing_user_id")),
        when(~r_session, lit("missing_session_id")),
        when(~r_ingest_ts, lit("bad_ingest_time")),
        when(~r_event_ts, lit("bad_event_time")),
        when(~b_product_for_click_cart, lit("product_id_required_for_click_or_cart")),
        when(~b_cart_fields, lit("add_to_cart_requires_qty_price_currency")),
        when(~b_purchase_fields, lit("purchase_requires_order_qty_price_currency")),
    )

    # Keep only non-null reasons as a single string column
    df = df.withColumn("validation_errors",
        concat_ws(",", reasons)
    )
    # A record is valid if there are NO errors (concat_ws returns "" when all are null)
    df = df.withColumn("is_valid", (col("validation_errors") == lit("")))

    # Quarantine output (invalid)
    quarantine_df = (
        df.filter(~col("is_valid"))
        # partition quarantine by ingest arrival
        .withColumn("ingest_date", to_date(col("ingest_time_ts")))
        .withColumn("ingest_hour", hour(col("ingest_time_ts")))
        .select(
            "raw_json",
            "validation_errors",
            "processing_time",
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_key",
            "event_id", "event_type", "user_id", "session_id", "event_time_ts", "ingest_time_ts",
            "ingest_date", "ingest_hour"
        )
    )

    q_quarantine = (
        quarantine_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", QUARANTINE_PATH)
        .option("checkpointLocation", QUARANTINE_CHECKPOINT)
        .partitionBy("ingest_date", "ingest_hour")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    # Silver output (valid, watermark, dedupe)
    silver_df = (
        df.filter(col("is_valid"))
        .withColumn("event_date", to_date(col("event_time_ts")))
        .withColumn("event_hour", hour(col("event_time_ts")))
        .withWatermark("event_time_ts", EVENT_WATERMARK)
        .dropDuplicates(["event_id"])
        .drop("validation_errors", "is_valid")
    )

    q_silver = (
        silver_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .partitionBy("event_date", "event_hour")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
