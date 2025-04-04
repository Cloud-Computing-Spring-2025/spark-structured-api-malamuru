from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col, avg 

# Start Spark session
spark = SparkSession.builder \
    .appName("Task2avg_listen_time_per_song") \
    .getOrCreate()

# Load enriched logs from output
enriched_logs = spark.read.option("header", True).csv("output/enriched_logs")

# Compute average listen time per song
avg_duration = enriched_logs.groupBy("song_id", "title") \
                            .agg(avg("duration_sec").alias("avg_listen_time"))

# Save result
avg_duration.write.format("csv").option("header", True).save("output/task2_avg_listen_time_per_song")

# Stop session
spark.stop()