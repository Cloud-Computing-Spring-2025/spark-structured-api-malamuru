from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, row_number, monotonically_increasing_id

# Start Spark session
spark = SparkSession.builder \
    .appName("Task4HappyRecommendations") \
    .getOrCreate()

# Load enriched logs and song metadata
enriched_logs = spark.read.option("header", True).csv("output/enriched_logs")
songs_df = spark.read.option("header", True).csv("input/songs_metadata.csv")

# Users who mostly listen to "Sad"
sad_plays = enriched_logs.groupBy("user_id", "mood").count()
sad_max = sad_plays.groupBy("user_id").agg(max("count").alias("max_count"))

sad_users = sad_plays.join(sad_max, on="user_id") \
                     .filter((col("mood") == "Sad") & (col("count") == col("max_count"))) \
                     .select("user_id")

# Songs already played
user_song_history = enriched_logs.select("user_id", "song_id").distinct()

# Happy songs
happy_songs = songs_df.filter(col("mood") == "Happy").select("song_id", "title", "artist")

# Recommend Happy songs user hasn’t played
recommendations = sad_users.crossJoin(happy_songs) \
    .join(user_song_history, ["user_id", "song_id"], "left_anti") \
    .dropDuplicates(["user_id", "song_id"]) \
    .withColumn("row_num", row_number().over(Window.partitionBy("user_id").orderBy(monotonically_increasing_id()))) \
    .filter(col("row_num") <= 3) \
    .drop("row_num")

# Save output
recommendations.write.format("csv").option("header", True).save("output/task4_happy_recommendations")

# Stop Spark session
spark.stop()