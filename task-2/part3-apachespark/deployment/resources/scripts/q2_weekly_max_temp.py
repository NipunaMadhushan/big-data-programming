# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, avg, year, month, weekofyear, round, concat_ws, regexp_extract
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Weekly Maximum Temperature Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Read weather data
weather_df = spark.read.csv(
    "hdfs://namenode:9000/user/task/data/weatherData.csv",
    header=True,
    inferSchema=True
)

# Read location data
location_df = spark.read.csv(
    "hdfs://namenode:9000/user/task/data/locationData.csv",
    header=True,
    inferSchema=True
)

# Extract month, year, and week from date string
weather_df = weather_df.withColumn(
    "month", 
    regexp_extract(col("date"), r"^(\d+)/", 1).cast("int")
).withColumn(
    "year",
    regexp_extract(col("date"), r"/(\d+)$", 1).cast("int")
).withColumn(
    "day",
    regexp_extract(col("date"), r"^(\d+)/(\d+)/", 2).cast("int")
)

# Calculate week number within the year (approximate)
# Week number = (day of year) / 7
from pyspark.sql.functions import dayofyear, floor

weather_df = weather_df.withColumn(
    "week_of_year",
    floor(dayofyear(col("date")) / 7).cast("int")
)

# Join with location data
weather_with_location = weather_df.join(
    location_df.select("location_id", "city_name"),
    on="location_id",
    how="inner"
)

# Step 1: Find the hottest months for each year
# (Month with highest average temperature_2m_max)
hottest_months = weather_with_location.groupBy("year", "month").agg(
    avg("`temperature_2m_max (°C)`").alias("avg_max_temp")
)

# Rank months within each year
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("year").orderBy(col("avg_max_temp").desc())

hottest_months = hottest_months.withColumn(
    "rank",
    row_number().over(window_spec)
).filter(col("rank") == 1).select("year", "month")

print("\n=== Hottest Months by Year ===")
hottest_months.show(20)

# Step 2: Get weekly maximum temperatures for these hottest months
weekly_max_temps = weather_with_location.join(
    hottest_months,
    on=["year", "month"],
    how="inner"
).groupBy("year", "month", "week_of_year").agg(
    max("`temperature_2m_max (°C)`").alias("weekly_max_temperature")
).withColumn(
    "year_month",
    concat_ws("-", col("year"), col("month"))
).select(
    "year",
    "month",
    "week_of_year",
    "weekly_max_temperature"
).orderBy("year", "month", "week_of_year")

print("\n=== Weekly Maximum Temperatures for Hottest Months ===")
weekly_max_temps.show(100, truncate=False)

# Save results to HDFS
weekly_max_temps.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "hdfs://namenode:9000/user/task/output/spark/q2_weekly_max_temp"
)

print("\nResults saved to: hdfs://namenode:9000/user/task/output/spark/q2_weekly_max_temp")

# Stop Spark session
spark.stop()
