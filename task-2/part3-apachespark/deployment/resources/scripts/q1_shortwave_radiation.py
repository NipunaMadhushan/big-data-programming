# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, round, when, regexp_extract

spark = SparkSession.builder.appName("Shortwave Radiation Analysis").config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000").getOrCreate()
weather_df = spark.read.csv("hdfs://namenode:9000/user/task/data/weatherData.csv", header=True, inferSchema=True)

# Rename problematic column
for c in weather_df.columns:
    if 'shortwave' in c:
        weather_df = weather_df.withColumnRenamed(c, "shortwave_radiation_sum")

# Extract month and year from date
weather_df = weather_df.withColumn("month", regexp_extract(col("date"), r"^(\d+)/", 1).cast("int"))
weather_df = weather_df.withColumn("year", regexp_extract(col("date"), r"/(\d+)$", 1).cast("int"))

# Calculate percentage of shortwave radiation > 15 MJ/m2 per month across ALL districts
radiation_analysis = weather_df.groupBy("year", "month").agg(
    count("*").alias("total_records"),
    sum(when(col("shortwave_radiation_sum") > 15, 1).otherwise(0)).alias("high_radiation_count")
).withColumn(
    "percentage",
    round((col("high_radiation_count") / col("total_records")) * 100, 2)
).select(
    "year",
    "month",
    "percentage"
).orderBy("year", "month")

print("\n=== Percentage of Shortwave Radiation > 15 MJ/m2 per Month ===")
radiation_analysis.show(200, truncate=False)

radiation_analysis.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/user/task/output/spark/q1_shortwave_radiation")
print("\nResults saved to: hdfs://namenode:9000/user/task/output/spark/q1_shortwave_radiation")

# Stop Spark session
spark.stop()
