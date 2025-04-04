from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col

# Start Spark session
spark = SparkSession.builder \
    .appName("Task1UserFavoriteGenres") \
    .getOrCreate()

# Load enriched logs from output
enriched_logs = spark.read.option("header", True).csv("output/enriched_logs")

# Group by user and genre, count plays
genre_counts = enriched_logs.groupBy("user_id", "genre").count()

# Use window function to find top genre per user
window_spec = Window.partitionBy("user_id").orderBy(desc("count"))
favorite_genres = genre_counts.withColumn("rank", row_number().over(window_spec)) \
                              .filter(col("rank") == 1) \
                              .drop("rank")

# Save output
favorite_genres.write.format("csv").option("header", True).save("output/task1_user_favorite_genres")

# Stop session
spark.stop()