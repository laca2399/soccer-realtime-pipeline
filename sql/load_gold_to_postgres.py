from pyspark.sql import SparkSession

# ----------------------------
# Spark Session (Delta + S3)
# ----------------------------

spark = (
    SparkSession.builder
    .appName("LoadGoldToPostgres")
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

# ----------------------------
# S3 Gold paths
# ----------------------------

MATCH_STATS_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/gold/match_stats"
TEAM_METRICS_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/gold/team_metrics"
PLAYER_METRICS_PATH = "s3a://soccer-realtime-pipeline-lake-lacayo/gold/player_metrics"

# ----------------------------
# Read Delta tables
# ----------------------------

match_df = spark.read.format("delta").load(MATCH_STATS_PATH)
team_df = spark.read.format("delta").load(TEAM_METRICS_PATH)
player_df = spark.read.format("delta").load(PLAYER_METRICS_PATH)

# ----------------------------
# PostgreSQL connection
# ----------------------------

jdbc_url = "jdbc:postgresql://localhost:5432/soccer_analytics"

connection_properties = {
    "user": "soccer",
    "password": "soccer",
    "driver": "org.postgresql.Driver"
}

# ----------------------------
# Write tables to PostgreSQL
# ----------------------------

match_df.write.jdbc(
    url=jdbc_url,
    table="match_stats",
    mode="overwrite",
    properties=connection_properties
)

team_df.write.jdbc(
    url=jdbc_url,
    table="team_metrics",
    mode="overwrite",
    properties=connection_properties
)

player_df.write.jdbc(
    url=jdbc_url,
    table="player_metrics",
    mode="overwrite",
    properties=connection_properties
)

print("Gold tables loaded into PostgreSQL")
