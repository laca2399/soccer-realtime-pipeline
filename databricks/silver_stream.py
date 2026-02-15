from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import split, trim

spark = (
    SparkSession.builder
    .appName("SoccerSilverStreaming")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Read Bronze Stream
# ----------------------------

bronze_stream = (
    spark.readStream
    .format("delta")
    .load("data/bronze")
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
# Write Silver Table
# ----------------------------

query = (
    silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "data/checkpoints/silver")
    .start("data/silver")
)

print("Silver streaming started")

query.awaitTermination()
