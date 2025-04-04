from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

# Start Spark session
spark = SparkSession.builder \
    .appName("Task5GenreLoyaltyScores") \
    .getOrCreate()

# Load enriched logs
enriched_logs = spark.read.option("header", True).csv("output/enriched_logs")

# Count plays per user per genre
user_genre_counts = enriched_logs.groupBy("user_id", "genre").count()

# Total plays per user
total_plays = enriched_logs.groupBy("user_id").count().withColumnRenamed("count", "total")

# Most played genre count per user
max_genre_plays = user_genre_counts.groupBy("user_id").agg(max("count").alias("max_count"))

# Calculate loyalty score
loyalty = max_genre_plays.join(total_plays, on="user_id") \
                         .withColumn("loyalty_score", col("max_count") / col("total")) \
                         .filter(col("loyalty_score") > 0.8)

# Save result
loyalty.write.format("csv").option("header", True).save("output/task5_genre_loyalty_scores")

# Stop session
spark.stop()