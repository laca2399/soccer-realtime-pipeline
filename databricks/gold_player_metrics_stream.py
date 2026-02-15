from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when

spark = (
    SparkSession.builder
    .appName("GoldPlayerMetrics")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Read Silver Stream
# ----------------------------

silver_stream = (
    spark.readStream
    .format("delta")
    .load("data/silver")
)

# ----------------------------
# Player Metrics Aggregation
# ----------------------------

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

# ----------------------------
# Write Gold Table
# ----------------------------

query = (
    player_metrics.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "data/checkpoints/gold_player_metrics")
    .start("data/gold/player_metrics")
)

print("Gold player metrics streaming started")

query.awaitTermination()
