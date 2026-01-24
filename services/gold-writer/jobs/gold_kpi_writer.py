import os, time
import psycopg # psycopg for Postgres writes
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, sum as Fsum, when, lit, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType
from datetime import datetime, timezone

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio12345")

SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lake/silver/ecom_events/v1/")

CHECKPOINT = os.getenv("GOLD_CHECKPOINT", "s3a://lake/_checkpoints/gold/kpi_minute_v1/")

WATERMARK = os.getenv("EVENT_WATERMARK", "1 hour")
TRIGGER = os.getenv("TRIGGER", "10 seconds")
MAX_FILES_PER_TRIGGER = os.getenv("MAX_FILES_PER_TRIGGER", "15")

# Postgres
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/analytics")

silver_schema = StructType([
    StructField("event_time_ts", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("event_date", DateType(), True),
    StructField("event_hour", IntegerType(), True),
])

def s3a_path_exists(spark: SparkSession, uri: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(uri), hconf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(uri))

def wait_for_path(spark: SparkSession, uri: str) -> None:
    poll = int(os.getenv("WAIT_FOR_SILVER_POLL_SECONDS", "5"))
    timeout = int(os.getenv("WAIT_FOR_SILVER_SECONDS", "300"))

    start = time.time()
    while True:
        if s3a_path_exists(spark, uri):
            print(f"[INFO] Found input path: {uri}")
            return

        elapsed = int(time.time() - start)
        print(f"[INFO] Input path not found yet: {uri} | waiting... ({elapsed}s)")
        time.sleep(poll)

        if timeout > 0 and elapsed >= timeout:
            raise RuntimeError(f"[ERROR] Silver path still missing after {timeout}s: {uri}")

def build_spark():
    spark = (
        SparkSession.builder
        .appName("gold-kpi-writer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "16"))
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

def write_batch_to_postgres(batch_df, batch_id: int):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    # KPI batches are tiny; collect to driver is fine.
    rows = batch_df.select("window_start", "metric", "value").collect()
    print(f"[DEBUG] {ts} | Batch {batch_id} has {len(rows)} rows")
    if not rows:
        return

    window_starts = sorted({r["window_start"] for r in rows})

    payload = [(r["window_start"], r["metric"], float(r["value"])) for r in rows]

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # delete all metrics for those windows, then insert recalculated values
            cur.execute(
                "DELETE FROM serving.kpi_minute WHERE window_start = ANY(%s);",
                (window_starts,)
            )
            cur.executemany(
                "INSERT INTO serving.kpi_minute(window_start, metric, value) VALUES (%s, %s, %s);",
                payload
            )
        conn.commit()

def main():
    spark = build_spark()

    wait_for_path(spark, SILVER_PATH)

    silver = (
        spark.readStream.format("parquet")
        .schema(silver_schema)
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .option("basePath", SILVER_PATH)
        .load(SILVER_PATH)
    )

    df = (
        silver
        .filter(col("event_time_ts").isNotNull())
        .withWatermark("event_time_ts", WATERMARK)
    )

    # 1-minute event-time windows
    w = window(col("event_time_ts"), "1 minute")

    agg = (
        df.groupBy(w.alias("w"))
        .agg(
            Fsum(lit(1)).alias("events_total"),
            Fsum(when(col("event_type") == "click", 1).otherwise(0)).alias("events_click"),
            Fsum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("events_add_to_cart"),
            Fsum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("events_checkout"),
            Fsum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("events_purchase"),
            Fsum(when(col("event_type") == "purchase", (col("price") * col("quantity").cast("double"))).otherwise(lit(0.0))).alias("revenue"),
        )
        .select(
            col("w.start").alias("window_start"),
            col("events_total"),
            col("events_click"),
            col("events_add_to_cart"),
            col("events_checkout"),
            col("events_purchase"),
            col("revenue"),
            when(col("events_purchase") > 0, col("revenue") / col("events_purchase")).otherwise(lit(0.0)).alias("aov"),
            when(col("events_click") > 0, col("events_add_to_cart") / col("events_click")).otherwise(lit(0.0)).alias("add_to_cart_rate"),
            when(col("events_add_to_cart") > 0, col("events_checkout") / col("events_add_to_cart")).otherwise(lit(0.0)).alias("checkout_rate"),
            when(col("events_click") > 0, col("events_purchase") / col("events_click")).otherwise(lit(0.0)).alias("purchase_conversion"),
        )
    )

    # long format for Postgres: (window_start, metric, value)
    # stack(n, 'name', col, ...) creates n rows per window
    long_kpis = agg.selectExpr(
        "window_start",
        """
        stack(
          10,
          'events_total', cast(events_total as double),
          'events_click', cast(events_click as double),
          'events_add_to_cart', cast(events_add_to_cart as double),
          'events_checkout', cast(events_checkout as double),
          'events_purchase', cast(events_purchase as double),
          'revenue', cast(revenue as double),
          'aov', cast(aov as double),
          'add_to_cart_rate', cast(add_to_cart_rate as double),
          'checkout_rate', cast(checkout_rate as double),
          'purchase_conversion', cast(purchase_conversion as double)
        ) as (metric, value)
        """
    )

    query = (
        long_kpis.writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
