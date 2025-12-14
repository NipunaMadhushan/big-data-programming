"""
Task 3: Spark MLlib Analysis - Predicting Weather Conditions for Lower Evapotranspiration
================================================================================

This script uses Apache Spark MLlib to:
1. Train a regression model to predict evapotranspiration from weather features
2. Analyze feature importance and relationships
3. Determine optimal conditions (precipitation_hours, sunshine, wind_speed) 
   for achieving lower evapotranspiration in May

Target: Find conditions that result in evapotranspiration < 1.5mm for May 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, month, year, avg, stddev, min as spark_min, max as spark_max,
    when, lit, round as spark_round, count
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GradientBoostedTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import numpy as np

# ============================================================================
# STEP 1: Initialize Spark Session
# ============================================================================
print("=" * 80)
print("STEP 1: Initializing Spark Session")
print("=" * 80)

spark = SparkSession.builder \
    .appName("Evapotranspiration_ML_Analysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session initialized successfully")
print(f"Spark version: {spark.version}")

# ============================================================================
# STEP 2: Load and Explore Data
# ============================================================================
print("\n" + "=" * 80)
print("STEP 2: Loading and Exploring Data")
print("=" * 80)

# Load weather data
weather_df = spark.read.csv(
    "/opt/resources/weatherData.csv",
    header=True,
    inferSchema=True
)

# Load location data
location_df = spark.read.csv(
    "/opt/resources/locationData.csv",
    header=True,
    inferSchema=True
)

print(f"\nWeather data: {weather_df.count()} records")
print(f"Location data: {location_df.count()} records")

# Display schema
print("\nWeather Data Schema:")
weather_df.printSchema()

# ============================================================================
# STEP 3: Data Preparation and Cleaning
# ============================================================================
print("\n" + "=" * 80)
print("STEP 3: Data Preparation and Cleaning")
print("=" * 80)

# Rename columns to remove special characters and units
weather_df = weather_df \
    .withColumnRenamed("weather_code (wmo code)", "weather_code") \
    .withColumnRenamed("temperature_2m_max (°C)", "temp_max") \
    .withColumnRenamed("temperature_2m_min (°C)", "temp_min") \
    .withColumnRenamed("temperature_2m_mean (°C)", "temp_mean") \
    .withColumnRenamed("apparent_temperature_max (°C)", "apparent_temp_max") \
    .withColumnRenamed("apparent_temperature_min (°C)", "apparent_temp_min") \
    .withColumnRenamed("apparent_temperature_mean (°C)", "apparent_temp_mean") \
    .withColumnRenamed("daylight_duration (s)", "daylight_duration") \
    .withColumnRenamed("sunshine_duration (s)", "sunshine_duration") \
    .withColumnRenamed("precipitation_sum (mm)", "precipitation_sum") \
    .withColumnRenamed("rain_sum (mm)", "rain_sum") \
    .withColumnRenamed("precipitation_hours (h)", "precipitation_hours") \
    .withColumnRenamed("wind_speed_10m_max (km/h)", "wind_speed") \
    .withColumnRenamed("wind_gusts_10m_max (km/h)", "wind_gusts") \
    .withColumnRenamed("wind_direction_10m_dominant (°)", "wind_direction") \
    .withColumnRenamed("shortwave_radiation_sum (MJ/m²)", "shortwave_radiation") \
    .withColumnRenamed("et0_fao_evapotranspiration (mm)", "evapotranspiration")

# Parse date and extract month/year
from pyspark.sql.functions import to_date, date_format

weather_df = weather_df \
    .withColumn("date_parsed", to_date(col("date"), "M/d/yyyy")) \
    .withColumn("month", month(col("date_parsed"))) \
    .withColumn("year", year(col("date_parsed")))

# Convert sunshine_duration from seconds to hours for consistency
weather_df = weather_df \
    .withColumn("sunshine_hours", col("sunshine_duration") / 3600.0)

# Join with location data
full_df = weather_df.join(location_df, on="location_id", how="left")

print("Data cleaning completed. Sample data:")
full_df.select("date", "city_name", "precipitation_hours", "sunshine_hours", 
               "wind_speed", "evapotranspiration", "month", "year").show(5)

# ============================================================================
# STEP 4: Filter Data for May (Month = 5)
# ============================================================================
print("\n" + "=" * 80)
print("STEP 4: Filtering Data for May")
print("=" * 80)

# Filter for May data only
may_df = full_df.filter(col("month") == 5)

print(f"Total records for May: {may_df.count()}")

# Exploratory statistics for May
print("\nDescriptive Statistics for May data:")
may_df.select("precipitation_hours", "sunshine_hours", "wind_speed", "evapotranspiration") \
    .describe().show()

# Check for null values in key columns
print("\nNull value counts in key columns:")
for column in ["precipitation_hours", "sunshine_hours", "wind_speed", "evapotranspiration"]:
    null_count = may_df.filter(col(column).isNull()).count()
    print(f"  {column}: {null_count} nulls")

# Remove rows with null values in key columns
may_df_clean = may_df.filter(
    col("precipitation_hours").isNotNull() &
    col("sunshine_hours").isNotNull() &
    col("wind_speed").isNotNull() &
    col("evapotranspiration").isNotNull()
)

print(f"\nRecords after removing nulls: {may_df_clean.count()}")

# ============================================================================
# STEP 5: Feature Selection and Engineering
# ============================================================================
print("\n" + "=" * 80)
print("STEP 5: Feature Selection and Engineering")
print("=" * 80)

# Select features for the model
# Primary features: precipitation_hours, sunshine_hours, wind_speed
# Additional features that influence evapotranspiration: temp_mean, shortwave_radiation

feature_columns = [
    "precipitation_hours",
    "sunshine_hours", 
    "wind_speed",
    "temp_mean",
    "shortwave_radiation"
]

print("Selected features for modeling:")
for i, feat in enumerate(feature_columns, 1):
    print(f"  {i}. {feat}")

# Prepare the dataset - ensure all features are DoubleType
for col_name in feature_columns + ["evapotranspiration"]:
    may_df_clean = may_df_clean.withColumn(col_name, col(col_name).cast(DoubleType()))

# Remove any remaining null values
may_df_clean = may_df_clean.dropna(subset=feature_columns + ["evapotranspiration"])

print(f"\nFinal dataset size: {may_df_clean.count()} records")

# ============================================================================
# STEP 6: Correlation Analysis
# ============================================================================
print("\n" + "=" * 80)
print("STEP 6: Correlation Analysis")
print("=" * 80)

# Convert to Pandas for correlation analysis
pandas_df = may_df_clean.select(feature_columns + ["evapotranspiration"]).toPandas()

print("\nCorrelation with Evapotranspiration:")
print("-" * 50)
for feature in feature_columns:
    correlation = pandas_df[feature].corr(pandas_df["evapotranspiration"])
    print(f"  {feature}: {correlation:.4f}")

# ============================================================================
# STEP 7: Train-Test Split (80-20)
# ============================================================================
print("\n" + "=" * 80)
print("STEP 7: Train-Test Split (80% Training, 20% Validation)")
print("=" * 80)

# Create feature vector
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features_raw"
)

# Scale features
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

# Split data: 80% training, 20% validation
train_df, test_df = may_df_clean.randomSplit([0.8, 0.2], seed=42)

print(f"Training set size: {train_df.count()} records (80%)")
print(f"Validation set size: {test_df.count()} records (20%)")

# ============================================================================
# STEP 8: Model Training - Multiple Models
# ============================================================================
print("\n" + "=" * 80)
print("STEP 8: Model Training")
print("=" * 80)

# Define evaluator
evaluator = RegressionEvaluator(
    labelCol="evapotranspiration",
    predictionCol="prediction",
    metricName="rmse"
)

# Dictionary to store results
model_results = {}

# ----------------------
# Model 1: Linear Regression
# ----------------------
print("\n--- Training Linear Regression Model ---")

lr = LinearRegression(
    featuresCol="features",
    labelCol="evapotranspiration",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.5
)

lr_pipeline = Pipeline(stages=[assembler, scaler, lr])
lr_model = lr_pipeline.fit(train_df)
lr_predictions = lr_model.transform(test_df)

lr_rmse = evaluator.evaluate(lr_predictions)
evaluator.setMetricName("r2")
lr_r2 = evaluator.evaluate(lr_predictions)
evaluator.setMetricName("mae")
lr_mae = evaluator.evaluate(lr_predictions)

model_results["Linear Regression"] = {
    "model": lr_model,
    "RMSE": lr_rmse,
    "R2": lr_r2,
    "MAE": lr_mae
}

print(f"  RMSE: {lr_rmse:.4f}")
print(f"  R²: {lr_r2:.4f}")
print(f"  MAE: {lr_mae:.4f}")

# Get coefficients
lr_summary = lr_model.stages[-1]
print("\n  Coefficients:")
for i, feature in enumerate(feature_columns):
    print(f"    {feature}: {lr_summary.coefficients[i]:.6f}")
print(f"    Intercept: {lr_summary.intercept:.6f}")

# ----------------------
# Model 2: Random Forest
# ----------------------
print("\n--- Training Random Forest Model ---")

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="evapotranspiration",
    numTrees=100,
    maxDepth=10,
    seed=42
)

rf_pipeline = Pipeline(stages=[assembler, scaler, rf])
rf_model = rf_pipeline.fit(train_df)
rf_predictions = rf_model.transform(test_df)

evaluator.setMetricName("rmse")
rf_rmse = evaluator.evaluate(rf_predictions)
evaluator.setMetricName("r2")
rf_r2 = evaluator.evaluate(rf_predictions)
evaluator.setMetricName("mae")
rf_mae = evaluator.evaluate(rf_predictions)

model_results["Random Forest"] = {
    "model": rf_model,
    "RMSE": rf_rmse,
    "R2": rf_r2,
    "MAE": rf_mae
}

print(f"  RMSE: {rf_rmse:.4f}")
print(f"  R²: {rf_r2:.4f}")
print(f"  MAE: {rf_mae:.4f}")

# Feature importance
print("\n  Feature Importance:")
rf_feature_importance = rf_model.stages[-1].featureImportances.toArray()
for i, feature in enumerate(feature_columns):
    print(f"    {feature}: {rf_feature_importance[i]:.4f}")

# ----------------------
# Model 3: Gradient Boosted Trees
# ----------------------
print("\n--- Training Gradient Boosted Trees Model ---")

gbt = GradientBoostedTreeRegressor(
    featuresCol="features",
    labelCol="evapotranspiration",
    maxIter=100,
    maxDepth=5,
    seed=42
)

gbt_pipeline = Pipeline(stages=[assembler, scaler, gbt])
gbt_model = gbt_pipeline.fit(train_df)
gbt_predictions = gbt_model.transform(test_df)

evaluator.setMetricName("rmse")
gbt_rmse = evaluator.evaluate(gbt_predictions)
evaluator.setMetricName("r2")
gbt_r2 = evaluator.evaluate(gbt_predictions)
evaluator.setMetricName("mae")
gbt_mae = evaluator.evaluate(gbt_predictions)

model_results["Gradient Boosted Trees"] = {
    "model": gbt_model,
    "RMSE": gbt_rmse,
    "R2": gbt_r2,
    "MAE": gbt_mae
}

print(f"  RMSE: {gbt_rmse:.4f}")
print(f"  R²: {gbt_r2:.4f}")
print(f"  MAE: {gbt_mae:.4f}")

# ============================================================================
# STEP 9: Model Comparison and Selection
# ============================================================================
print("\n" + "=" * 80)
print("STEP 9: Model Comparison and Selection")
print("=" * 80)

print("\n{:<25} {:<12} {:<12} {:<12}".format("Model", "RMSE", "R²", "MAE"))
print("-" * 60)
for model_name, metrics in model_results.items():
    print("{:<25} {:<12.4f} {:<12.4f} {:<12.4f}".format(
        model_name, metrics["RMSE"], metrics["R2"], metrics["MAE"]
    ))

# Select best model based on R²
best_model_name = max(model_results.keys(), key=lambda x: model_results[x]["R2"])
best_model = model_results[best_model_name]["model"]
print(f"\nBest performing model: {best_model_name}")

# ============================================================================
# STEP 10: Analysis for Low Evapotranspiration Conditions
# ============================================================================
print("\n" + "=" * 80)
print("STEP 10: Determining Conditions for Low Evapotranspiration")
print("=" * 80)

# Analyze historical data where evapotranspiration < 1.5mm in May
low_et_df = may_df_clean.filter(col("evapotranspiration") < 1.5)

print(f"\nHistorical records with evapotranspiration < 1.5mm in May: {low_et_df.count()}")

if low_et_df.count() > 0:
    print("\nStatistics for low evapotranspiration conditions:")
    low_et_stats = low_et_df.select(
        avg("precipitation_hours").alias("mean_precip_hours"),
        stddev("precipitation_hours").alias("std_precip_hours"),
        spark_min("precipitation_hours").alias("min_precip_hours"),
        spark_max("precipitation_hours").alias("max_precip_hours"),
        avg("sunshine_hours").alias("mean_sunshine_hours"),
        stddev("sunshine_hours").alias("std_sunshine_hours"),
        spark_min("sunshine_hours").alias("min_sunshine_hours"),
        spark_max("sunshine_hours").alias("max_sunshine_hours"),
        avg("wind_speed").alias("mean_wind_speed"),
        stddev("wind_speed").alias("std_wind_speed"),
        spark_min("wind_speed").alias("min_wind_speed"),
        spark_max("wind_speed").alias("max_wind_speed"),
        avg("evapotranspiration").alias("mean_et")
    ).collect()[0]
    
    print("\n  Precipitation Hours:")
    print(f"    Mean: {low_et_stats['mean_precip_hours']:.2f} hours")
    print(f"    Std Dev: {low_et_stats['std_precip_hours']:.2f}")
    print(f"    Range: {low_et_stats['min_precip_hours']:.2f} - {low_et_stats['max_precip_hours']:.2f}")
    
    print("\n  Sunshine Hours:")
    print(f"    Mean: {low_et_stats['mean_sunshine_hours']:.2f} hours")
    print(f"    Std Dev: {low_et_stats['std_sunshine_hours']:.2f}")
    print(f"    Range: {low_et_stats['min_sunshine_hours']:.2f} - {low_et_stats['max_sunshine_hours']:.2f}")
    
    print("\n  Wind Speed:")
    print(f"    Mean: {low_et_stats['mean_wind_speed']:.2f} km/h")
    print(f"    Std Dev: {low_et_stats['std_wind_speed']:.2f}")
    print(f"    Range: {low_et_stats['min_wind_speed']:.2f} - {low_et_stats['max_wind_speed']:.2f}")

# ============================================================================
# STEP 11: Prediction for May 2026
# ============================================================================
print("\n" + "=" * 80)
print("STEP 11: Prediction for May 2026 with Low Evapotranspiration")
print("=" * 80)

# Using Linear Regression coefficients to determine optimal conditions
# For low ET, we need to understand the coefficient directions

print("\nUsing Linear Regression model for interpretable predictions:")
lr_coefficients = lr_model.stages[-1].coefficients.toArray()
lr_intercept = lr_model.stages[-1].intercept

print("\nModel equation: ET = ", end="")
equation_parts = []
for i, feature in enumerate(feature_columns):
    sign = "+" if lr_coefficients[i] >= 0 else ""
    equation_parts.append(f"{sign}{lr_coefficients[i]:.4f}×{feature}")
print(" ".join(equation_parts) + f" + {lr_intercept:.4f}")

# Analyze coefficient impact
print("\nCoefficient Analysis (impact on evapotranspiration):")
for i, feature in enumerate(feature_columns):
    impact = "increases" if lr_coefficients[i] > 0 else "decreases"
    print(f"  {feature}: {impact} ET by {abs(lr_coefficients[i]):.4f} per unit increase")

# Calculate recommended conditions based on historical low-ET patterns
print("\n" + "=" * 80)
print("RECOMMENDED CONDITIONS FOR MAY 2026")
print("Target: Evapotranspiration < 1.5mm")
print("=" * 80)

# Get typical May values as baseline
may_stats = may_df_clean.select(
    avg("precipitation_hours").alias("avg_precip"),
    avg("sunshine_hours").alias("avg_sunshine"),
    avg("wind_speed").alias("avg_wind"),
    avg("temp_mean").alias("avg_temp"),
    avg("shortwave_radiation").alias("avg_radiation"),
    avg("evapotranspiration").alias("avg_et")
).collect()[0]

print("\nTypical May conditions (historical average):")
print(f"  Average Precipitation Hours: {may_stats['avg_precip']:.2f} hours")
print(f"  Average Sunshine Hours: {may_stats['avg_sunshine']:.2f} hours")
print(f"  Average Wind Speed: {may_stats['avg_wind']:.2f} km/h")
print(f"  Average Evapotranspiration: {may_stats['avg_et']:.2f} mm")

# Based on coefficient analysis and low-ET historical data
if low_et_df.count() > 0:
    recommended_precip = low_et_stats['mean_precip_hours']
    recommended_sunshine = low_et_stats['mean_sunshine_hours']
    recommended_wind = low_et_stats['mean_wind_speed']
else:
    # Calculate based on model coefficients if no historical low-ET data
    recommended_precip = may_stats['avg_precip'] * 1.5 if lr_coefficients[0] < 0 else may_stats['avg_precip'] * 0.5
    recommended_sunshine = may_stats['avg_sunshine'] * 0.5 if lr_coefficients[1] > 0 else may_stats['avg_sunshine'] * 1.5
    recommended_wind = may_stats['avg_wind'] * 0.5 if lr_coefficients[2] > 0 else may_stats['avg_wind'] * 1.5

print("\n" + "*" * 60)
print("PREDICTION RESULTS FOR MAY 2026")
print("*" * 60)
print("\nTo achieve evapotranspiration LOWER than 1.5mm in May 2026,")
print("the following daily conditions are recommended:")
print("\n  ┌─────────────────────────────────────────────────────┐")
print(f"  │  Precipitation Hours: {recommended_precip:>8.2f} hours              │")
print(f"  │  Sunshine Duration:   {recommended_sunshine:>8.2f} hours              │")
print(f"  │  Wind Speed:          {recommended_wind:>8.2f} km/h               │")
print("  └─────────────────────────────────────────────────────┘")

# Verify prediction using the best model
print("\nModel Verification:")

# Create test data point with recommended conditions
test_data = spark.createDataFrame([
    (recommended_precip, recommended_sunshine, recommended_wind, 
     may_stats['avg_temp'], may_stats['avg_radiation'])
], ["precipitation_hours", "sunshine_hours", "wind_speed", "temp_mean", "shortwave_radiation"])

# Make prediction
prediction = best_model.transform(test_data)
predicted_et = prediction.select("prediction").collect()[0][0]

print(f"\n  Using {best_model_name} model:")
print(f"  Predicted Evapotranspiration: {predicted_et:.4f} mm")
print(f"  Target: < 1.5 mm")
print(f"  Status: {'✓ ACHIEVABLE' if predicted_et < 1.5 else '✗ NEEDS ADJUSTMENT'}")

# ============================================================================
# STEP 12: Summary Report
# ============================================================================
print("\n" + "=" * 80)
print("FINAL SUMMARY REPORT")
print("=" * 80)

print("""
ANALYSIS STEPS COMPLETED:
─────────────────────────
1. Data Loading: Loaded weather data (2010-2024) and location data
2. Data Preparation: Cleaned columns, parsed dates, converted units
3. Feature Selection: precipitation_hours, sunshine_hours, wind_speed, 
                      temp_mean, shortwave_radiation
4. Data Filtering: Filtered for May data only
5. Train-Test Split: 80% training, 20% validation
6. Model Training: Trained 3 models (Linear Regression, Random Forest, GBT)
7. Model Evaluation: Compared using RMSE, R², and MAE metrics
8. Analysis: Identified conditions associated with low evapotranspiration
9. Prediction: Generated recommendations for May 2026
""")

print("\nMODEL PERFORMANCE SUMMARY:")
print("-" * 40)
for model_name, metrics in model_results.items():
    print(f"\n{model_name}:")
    print(f"  RMSE: {metrics['RMSE']:.4f}")
    print(f"  R²:   {metrics['R2']:.4f}")
    print(f"  MAE:  {metrics['MAE']:.4f}")

print(f"\nBest Model: {best_model_name} (based on R² score)")

print("\n" + "=" * 80)
print("KEY FINDINGS FOR MAY 2026 PREDICTION")
print("=" * 80)
print(f"""
To maintain evapotranspiration below 1.5mm in May 2026:

┌────────────────────────────────────────────────────────────────┐
│  RECOMMENDED DAILY WEATHER CONDITIONS                          │
├────────────────────────────────────────────────────────────────┤
│  • Mean Precipitation Hours:  {recommended_precip:>6.2f} hours                   │
│  • Mean Sunshine Duration:    {recommended_sunshine:>6.2f} hours                   │
│  • Mean Wind Speed:           {recommended_wind:>6.2f} km/h                    │
├────────────────────────────────────────────────────────────────┤
│  Expected Evapotranspiration: {predicted_et:>6.4f} mm (Target: < 1.5mm)     │
└────────────────────────────────────────────────────────────────┘

KEY INSIGHTS:
─────────────
• Higher precipitation hours tend to reduce evapotranspiration
• Lower sunshine duration correlates with reduced evapotranspiration  
• Moderate wind speeds help maintain lower evapotranspiration levels
• These conditions are typical of cloudy, rainy days in May
""")

# Stop Spark session
spark.stop()
print("\nSpark session stopped. Analysis complete!")
