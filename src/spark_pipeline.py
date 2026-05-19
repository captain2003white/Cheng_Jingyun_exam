from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SensorPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# ---------------- Kafka Source ----------------

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-events") \
    .option("startingOffsets", "earliest") \
    .load()

df_raw = df_kafka.selectExpr("CAST(value AS STRING)")

# ---------------- JSON Parsing ----------------

from pyspark.sql.types import StructType, StringType, DoubleType, LongType, BooleanType

schema = StructType() \
    .add("sensor", StringType()) \
    .add("value", DoubleType()) \
    .add("unit", StringType()) \
    .add("timestamp", LongType()) \
    .add("source", StringType()) \
    .add("anomaly", BooleanType())

from pyspark.sql.functions import from_json

df_parsed = df_raw.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ---------------- Event Time ----------------

df_parsed = df_parsed.withColumn(
    "event_time",
    (col("timestamp") / 1000).cast("timestamp")
)

# ---------------- Data Validation ----------------

df_clean = df_parsed.filter(
    (
        (col("sensor") == "temperature") & (col("value").between(15, 45))
    ) |
    (
        (col("sensor") == "humidity") & (col("value").between(30, 95))
    ) |
    (
        (col("sensor") == "pressure") & (col("value").between(980, 1040))
    )
)

# ---------------- Anomaly Detection ----------------

from pyspark.sql.functions import when

df_with_anomaly = df_clean.withColumn(
    "is_anomaly",
    when(col("sensor") == "temperature", col("value") > 35)
    .when(col("sensor") == "humidity", col("value") > 90)
    .when(col("sensor") == "pressure", (col("value") < 990) | (col("value") > 1030))
    .otherwise(False)
)

# ---------------- Watermark + Window Aggregation ----------------

from pyspark.sql.functions import window, avg, min, max, count, sum

df_agg = df_with_anomaly \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor")
    ) \
    .agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        count("*").alias("count"),
        sum(col("is_anomaly").cast("int")).alias("anomaly_count")
    )

# ---------------- Data Lake Paths ----------------

RAW_PATH = "/tmp/datalake/raw"
CURATED_PATH = "/tmp/datalake/curated"
CONSUMPTION_PATH = "/tmp/datalake/consumption"

# ---------------- Raw Zone ----------------
from pyspark.sql.functions import current_timestamp, year, month, dayofmonth, hour

df_raw_partitioned = df_raw \
    .withColumn("ingest_time", current_timestamp()) \
    .withColumn("year", year("ingest_time")) \
    .withColumn("month", month("ingest_time")) \
    .withColumn("day", dayofmonth("ingest_time")) \
    .withColumn("hour", hour("ingest_time"))

query_raw = df_raw_partitioned.writeStream \
    .format("json") \
    .option("path", RAW_PATH) \
    .option("checkpointLocation", "checkpoints/raw") \
    .partitionBy("year", "month", "day", "hour") \
    .outputMode("append") \
    .start()

# ---------------- Curated Zone ----------------

df_curated_ready = df_with_anomaly \
    .withColumnRenamed("sensor", "sensor_type") \
    .withColumn("year", year(col("event_time"))) \
    .withColumn("month", month(col("event_time"))) \
    .withColumn("day", dayofmonth(col("event_time")))

query_curated = df_curated_ready.writeStream \
    .format("parquet") \
    .option("path", CURATED_PATH) \
    .option("checkpointLocation", "checkpoints/curated") \
    .partitionBy("sensor_type", "year", "month", "day") \
    .outputMode("append") \
    .start()

# ---------------- Consumption Zone ----------------

df_consumption_ready = df_agg.withColumnRenamed("sensor", "sensor_type")

query_consumption = df_consumption_ready.writeStream \
    .format("parquet") \
    .option("path", CONSUMPTION_PATH) \
    .option("checkpointLocation", "checkpoints/consumption") \
    .partitionBy("sensor_type") \
    .outputMode("append") \
    .start()

# ---------------- Await termination ----------------

spark.streams.awaitAnyTermination()