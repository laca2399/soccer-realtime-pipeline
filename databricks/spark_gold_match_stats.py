from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("GoldMatchStats")
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
# Goal Events
# ----------------------------

goals = silver_stream.filter(col("event_type") == "GOAL")

# ----------------------------
# Match Aggregation
# ----------------------------

match_stats = (
    goals
    .groupBy("match_id", "team")
    .count()
    .withColumnRenamed("count", "goals")
)

# ----------------------------
# Write Gold Table
# ----------------------------

query = (
    match_stats.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "data/checkpoints/gold_match_stats")
    .start("data/gold/match_stats")
)

print("Gold match stats streaming started")

query.awaitTermination()
