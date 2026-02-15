from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# ----------------------------
# Spark Session (Delta + S3 enabled)
# ----------------------------

spark = (
    SparkSession.builder
    .appName("SoccerKafkaStreaming")
    .master("local[*]")

    # Delta Lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # S3A configuration
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Kafka config
# ----------------------------

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "soccer_events"

# ----------------------------
# S3 paths
# ----------------------------

BRONZE_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/bronze"
CHECKPOINT_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/checkpoints/bronze"

# ----------------------------
# Event schema
# ----------------------------

schema = StructType([
    StructField("event_id", StringType()),
    StructField("match_id", IntegerType()),
    StructField("league", StringType()),
    StructField("season", StringType()),
    StructField("minute", IntegerType()),
    StructField("event_type", StringType()),
    StructField("player", StringType()),
    StructField("team", StringType()),
    StructField("xg", DoubleType()),
    StructField("is_home", BooleanType()),
    StructField("event_time", StringType()),
    StructField("ingestion_time", StringType())
])

# ----------------------------
# Kafka stream
# ----------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# ----------------------------
# Write Bronze Delta to S3
# ----------------------------

query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(BRONZE_PATH)
)

print("Streaming started → Bronze Delta table on S3")

query.awaitTermination()


