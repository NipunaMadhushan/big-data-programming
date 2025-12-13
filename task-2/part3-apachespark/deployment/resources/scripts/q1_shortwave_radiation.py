from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, round, when, concat_ws, regexp_extract

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Shortwave Radiation Analysis") \
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

# Extract month and year from date string
# Date format is M/D/YYYY
weather_df = weather_df.withColumn(
    "month", 
    regexp_extract(col("date"), r"^(\d+)/", 1).cast("int")
).withColumn(
    "year",
    regexp_extract(col("date"), r"/(\d+)$", 1).cast("int")
)

# Join with location data to get city names
weather_with_location = weather_df.join(
    location_df.select("location_id", "city_name"),
    on="location_id",
    how="inner"
)

# Create year-month column
weather_with_location = weather_with_location.withColumn(
    "year_month",
    concat_ws("-", col("year"), col("month"))
)

# Calculate percentage of shortwave radiation > 15 MJ/m2
radiation_analysis = weather_with_location.groupBy("year_month").agg(
    count("*").alias("total_records"),
    sum(when(col("shortwave_radiation_sum (MJ/m2)") > 15, 1).otherwise(0)).alias("high_radiation_count")
).withColumn(
    "percentage",
    round((col("high_radiation_count") / col("total_records")) * 100, 2)
).select(
    "year_month",
    "total_records",
    "high_radiation_count",
    "percentage"
).orderBy("year_month")

# Show results
print("\n=== Percentage of Shortwave Radiation > 15 MJ/m2 per Month ===")
radiation_analysis.show(50, truncate=False)

# Save results to HDFS
radiation_analysis.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "hdfs://namenode:9000/user/task/output/spark/q1_shortwave_radiation"
)

print("\nResults saved to: hdfs://namenode:9000/user/task/output/spark/q1_shortwave_radiation")

# Stop Spark session
spark.stop()
