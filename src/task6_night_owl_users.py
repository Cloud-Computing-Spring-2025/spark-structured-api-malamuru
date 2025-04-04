from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col

# Start Spark session
spark = SparkSession.builder \
    .appName("Task6NightOwlUsers") \
    .getOrCreate()

# Load enriched logs
enriched_logs = spark.read.option("header", True).csv("output/enriched_logs")

# Extract hour from timestamp and filter for 12 AM to 5 AM
night_logs = enriched_logs.withColumn("hour", hour(col("timestamp"))) \
                          .filter((col("hour") >= 0) & (col("hour") < 5))

# Get distinct users
night_owls = night_logs.select("user_id").distinct()

# Save result
night_owls.write.format("csv").option("header", True).save("output/task6_night_owl_users")

# Stop Spark session
spark.stop()