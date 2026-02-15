from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import split, trim

# ----------------------------
# Spark Session (Delta + S3 enabled)
# ----------------------------

spark = (
    SparkSession.builder
    .appName("SoccerSilverStreaming")
    .master("local[*]")

    # Delta Lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # S3A support
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# S3 paths
# ----------------------------

BRONZE_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/bronze"
SILVER_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/silver"
CHECKPOINT_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/checkpoints/silver"

# ----------------------------
# Read Bronze Stream
# ----------------------------

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

# ----------------------------
# Transform to Silver
# ----------------------------

silver_stream = (
    bronze_stream
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))
    .withColumn("player", trim(split(col("player"), ",").getItem(0)))
    .filter(col("match_id").isNotNull())
)

# ----------------------------
# Write Silver Table to S3
# ----------------------------

query = (
    silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(SILVER_PATH)
)

print("Silver streaming started → S3")

query.awaitTermination()
