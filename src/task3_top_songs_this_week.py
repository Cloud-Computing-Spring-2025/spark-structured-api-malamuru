from pyspark.sql import SparkSession
from pyspark.sql.functions import weekofyear, desc, col

# Start Spark session
spark = SparkSession.builder \
    .appName("Task3TopSongsThisWeek") \
    .getOrCreate()

# Load enriched logs
enriched_logs = spark.read.option("header", True).csv("output/enriched_logs")

# Filter logs from the current week (week 12 in your case)
logs_this_week = enriched_logs.filter(weekofyear("timestamp") == 12)

# Get top 10 songs by play count
top_songs = logs_this_week.groupBy("song_id", "title") \
                          .count() \
                          .orderBy(desc("count")) \
                          .limit(10)

# Save output
top_songs.write.format("csv").option("header", True).save("output/task3_top_songs_this_week")

# Stop Spark session
spark.stop()