from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Start Spark session
spark = SparkSession.builder \
    .appName("EnrichLogs") \
    .getOrCreate()

# Load logs and metadata
logs_df = spark.read.option("header", True).csv("input/listening_logs.csv")
songs_df = spark.read.option("header", True).csv("input/songs_metadata.csv")

# Convert timestamp and duration to correct types
logs_df = logs_df.withColumn("timestamp", to_timestamp(col("timestamp"))) \
                 .withColumn("duration_sec", col("duration_sec").cast("int"))

# Join logs with metadata
enriched_logs = logs_df.join(songs_df, on="song_id", how="left")

# Save enriched logs to output folder
enriched_logs.write.option("header", True).csv("output/enriched_logs")

print("Enriched logs saved to output/enriched_logs")

# Stop session
spark.stop()