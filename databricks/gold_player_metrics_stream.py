from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when

spark = (
    SparkSession.builder
    .appName("GoldPlayerMetrics")
    .master("local[*]")

    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/silver"
GOLD_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/gold/player_metrics"
CHECKPOINT_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/checkpoints/gold_player_metrics"

silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)

player_metrics = (
    silver_stream
    .groupBy("player", "team")
    .agg(
        sum(when(col("event_type") == "GOAL", 1).otherwise(0)).alias("goals"),
        sum(when(col("event_type") == "SHOT", 1).otherwise(0)).alias("shots"),
        sum(when(col("event_type") == "FOUL", 1).otherwise(0)).alias("fouls"),
        sum(when(col("event_type") == "YELLOW_CARD", 1).otherwise(0)).alias("yellow_cards"),
        sum(when(col("event_type") == "RED_CARD", 1).otherwise(0)).alias("red_cards"),
        sum("xg").alias("total_xg")
    )
)

query = (
    player_metrics.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(GOLD_PATH)
)

print("Gold player metrics streaming started → S3")

query.awaitTermination()
