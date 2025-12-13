# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, avg, dayofyear, floor, regexp_extract, row_number, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Weekly Max Temp Analysis").config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000").getOrCreate()
weather_df = spark.read.csv("hdfs://namenode:9000/user/task/data/weatherData.csv", header=True, inferSchema=True)

# Rename problematic columns
for c in weather_df.columns:
    if 'temperature_2m_max' in c:
        weather_df = weather_df.withColumnRenamed(c, "temperature_2m_max")

# Extract month, year from date string (format: M/D/YYYY)
weather_df = weather_df.withColumn("month", regexp_extract(col("date"), r"^(\d+)/", 1).cast("int"))
weather_df = weather_df.withColumn("year", regexp_extract(col("date"), r"/(\d+)$", 1).cast("int"))
weather_df = weather_df.withColumn("day", regexp_extract(col("date"), r"^(\d+)/(\d+)/", 2).cast("int"))

# Calculate week number within each month (1-5 weeks per month)
# Week 1: days 1-7, Week 2: days 8-14, Week 3: days 15-21, Week 4: days 22-28, Week 5: days 29-31
weather_df = weather_df.withColumn("week_of_month", floor((col("day") - 1) / 7).cast("int") + 1)

print("\n=== Step 1: Finding Hottest Month for Each Year ===")
# Step 1: Find the hottest month for each year (month with highest average max temperature)
hottest_months = weather_df.groupBy("year", "month").agg(
    avg("temperature_2m_max").alias("avg_max_temp")
)

# Rank months within each year by average temperature (descending)
window_spec = Window.partitionBy("year").orderBy(col("avg_max_temp").desc())
hottest_months = hottest_months.withColumn("rank", row_number().over(window_spec))

# Keep only the hottest month (rank = 1) for each year
hottest_months = hottest_months.filter(col("rank") == 1).select("year", "month", "avg_max_temp")
hottest_months.show(20)

print("\n=== Step 2: Weekly Maximum Temperatures for Hottest Months ===")
# Step 2: Join back to get only data from hottest months, then calculate weekly max temps
weekly_max_temps = weather_df.join(
    hottest_months.select("year", "month"),
    on=["year", "month"],
    how="inner"
).groupBy("year", "month", "week_of_month").agg(
    max("temperature_2m_max").alias("weekly_max_temperature")
).select(
    "year",
    "month",
    "week_of_month",
    "weekly_max_temperature"
).orderBy("year", "month", "week_of_month")

weekly_max_temps.show(100, truncate=False)

# Save results
weekly_max_temps.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/user/task/output/spark/q2_weekly_max_temp")
print("\nResults saved to: hdfs://namenode:9000/user/task/output/spark/q2_weekly_max_temp")

# Stop Spark session
spark.stop()
