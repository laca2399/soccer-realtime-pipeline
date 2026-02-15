from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# ----------------------------
# Spark Session (Delta enabled)
# ----------------------------

spark = (
    SparkSession.builder
    .appName("SoccerKafkaStreaming")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "soccer_events"

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

query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "data/checkpoints/bronze")
    .start("data/bronze")
)

print("Streaming started → Bronze Delta table")

query.awaitTermination()


