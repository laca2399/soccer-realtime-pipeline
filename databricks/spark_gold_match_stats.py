from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("GoldMatchStats")
    .master("local[*]")

    # Delta
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # S3
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/silver"
GOLD_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/gold/match_stats"
CHECKPOINT_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/checkpoints/gold_match_stats"

silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)

goals = silver_stream.filter(col("event_type") == "GOAL")

match_stats = (
    goals
    .groupBy("match_id", "team")
    .count()
    .withColumnRenamed("count", "goals")
)

query = (
    match_stats.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(GOLD_PATH)
)

print("Gold match stats streaming started → S3")

query.awaitTermination()
