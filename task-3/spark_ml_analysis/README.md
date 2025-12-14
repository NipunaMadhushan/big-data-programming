# Task 3: Spark MLlib Analysis - Predicting Weather Conditions for Lower Evapotranspiration

## Objective

Determine the expected amount of **precipitation_hours**, **sunshine**, and **wind_speed** that would lead to a **lower amount of evapotranspiration** for the month of May using Apache Spark MLlib.

**Target**: Predict conditions for May 2026 to achieve evapotranspiration < 1.5mm

---

## Analysis Steps Overview

### Step 1: Data Loading
- Load weather data (2010-2024) containing meteorological observations
- Load location data with geographical details for all Sri Lankan districts
- Join datasets on `location_id`

### Step 2: Data Preparation and Cleaning
- Rename columns to remove special characters and units
- Parse date column and extract month/year
- Convert sunshine duration from seconds to hours
- Handle missing values

### Step 3: Feature Selection
Selected features for the model:
1. **precipitation_hours** - Duration of precipitation (hours)
2. **sunshine_hours** - Duration of sunshine (hours, converted from seconds)
3. **wind_speed** - Maximum wind speed at 10m (km/h)
4. **temp_mean** - Mean temperature (°C)
5. **shortwave_radiation** - Solar radiation (MJ/m²)

**Target Variable**: `et0_fao_evapotranspiration` (mm)

### Step 4: Data Filtering
- Filter dataset to include only May data (month = 5)
- Remove records with null values in key columns

### Step 5: Correlation Analysis
- Analyze correlation between features and evapotranspiration
- Identify which features have positive/negative relationships with the target

### Step 6: Train-Test Split
- **Training Set**: 80% of data
- **Validation Set**: 20% of data
- Random seed: 42 (for reproducibility)

### Step 7: Model Training
Three regression models are trained and compared:

1. **Linear Regression**
   - MaxIter: 100
   - RegParam: 0.01
   - ElasticNetParam: 0.5

2. **Random Forest Regressor**
   - Number of Trees: 100
   - Max Depth: 10

3. **Gradient Boosted Trees Regressor**
   - MaxIter: 100
   - Max Depth: 5

### Step 8: Model Evaluation
Models are evaluated using:
- **RMSE** (Root Mean Square Error)
- **R²** (Coefficient of Determination)
- **MAE** (Mean Absolute Error)

### Step 9: Low Evapotranspiration Analysis
- Filter historical data where evapotranspiration < 1.5mm
- Calculate mean, standard deviation, and range for each feature
- Identify optimal conditions

### Step 10: Prediction for May 2026
- Use the best performing model to predict evapotranspiration
- Verify if recommended conditions achieve target < 1.5mm

---

## Setup Instructions

### Prerequisites
- Docker and Docker Compose installed
- Data files: `weatherData.csv` and `locationData.csv`

### Directory Structure
```
spark_ml_analysis/
├── docker-compose.yml
├── resources/
│   ├── weatherData.csv
│   ├── locationData.csv
│   └── notebooks/
│       └── Task3_Evapotranspiration_ML_Analysis.json
├── scripts/
│   └── evapotranspiration_ml_analysis.py
└── README.md
```

### Step-by-Step Setup

1. **Create the project directory structure:**
```bash
mkdir -p spark_ml_analysis/resources/notebooks
mkdir -p spark_ml_analysis/scripts
```

2. **Copy the data files to resources folder:**
```bash
cp weatherData.csv spark_ml_analysis/resources/
cp locationData.csv spark_ml_analysis/resources/
```

3. **Copy the notebook JSON to resources/notebooks:**
```bash
cp Task3_Evapotranspiration_ML_Analysis.json spark_ml_analysis/resources/notebooks/
```

4. **Copy the Python script to scripts folder:**
```bash
cp evapotranspiration_ml_analysis.py spark_ml_analysis/scripts/
```

5. **Start the Docker container:**
```bash
cd spark_ml_analysis
docker-compose up -d
```

6. **Access Zeppelin:**
   - Open browser and navigate to: `http://localhost:8089`
   - No login required (anonymous access enabled by default)

7. **Import the notebook:**
   - Click on "Import note" in Zeppelin UI
   - Browse and select: `/opt/resources/notebooks/Task3_Evapotranspiration_ML_Analysis.json`
   - Or upload from your local machine
   - Run each paragraph sequentially (Shift+Enter)

---

## Alternative: Run Python Script Directly

If you prefer to run the analysis as a standalone Python script:

1. **Access the container:**
```bash
docker exec -it zeppelin bash
```

2. **Run the Python script:**
```bash
cd /opt/scripts
spark-submit evapotranspiration_ml_analysis.py
```

---

## Expected Output

### Model Performance Comparison
| Model                  | RMSE   | R²     | MAE    |
|------------------------|--------|--------|--------|
| Linear Regression      | ~0.XX  | ~0.XX  | ~0.XX  |
| Random Forest          | ~0.XX  | ~0.XX  | ~0.XX  |
| Gradient Boosted Trees | ~0.XX  | ~0.XX  | ~0.XX  |

### Recommended Conditions for May 2026
To achieve evapotranspiration < 1.5mm:

| Parameter            | Recommended Value |
|----------------------|-------------------|
| Precipitation Hours  | X.XX hours        |
| Sunshine Duration    | X.XX hours        |
| Wind Speed           | X.XX km/h         |

---

## Key Insights

1. **Precipitation Hours**: Higher values tend to reduce evapotranspiration (negative correlation)
2. **Sunshine Duration**: Higher sunshine typically increases evapotranspiration (positive correlation)
3. **Wind Speed**: Moderate wind speeds help regulate evapotranspiration
4. **Temperature**: Higher temperatures increase evapotranspiration significantly
5. **Solar Radiation**: Strong positive correlation with evapotranspiration

---

## Technical Notes

### Feature Engineering
- Sunshine duration is converted from seconds to hours for better interpretability
- All features are standardized using `StandardScaler` before model training
- `VectorAssembler` is used to combine features into a single vector column

### Model Selection Rationale
- **Linear Regression**: Provides interpretable coefficients to understand feature relationships
- **Random Forest**: Handles non-linear relationships and provides feature importance
- **Gradient Boosted Trees**: Often provides best predictive accuracy

### Evaluation Metrics
- **RMSE**: Measures average prediction error (lower is better)
- **R²**: Measures proportion of variance explained (higher is better, max 1.0)
- **MAE**: Average absolute prediction error (lower is better)

---

## Files Included

1. `docker-compose.yml` - Docker configuration for Zeppelin
2. `evapotranspiration_ml_analysis.py` - Complete PySpark ML script
3. `Task3_Evapotranspiration_ML_Analysis.json` - Zeppelin notebook
4. `README.md` - This documentation file

---

## Troubleshooting

### Common Issues

1. **Memory errors**: Increase Docker memory allocation or adjust Spark configuration
2. **File not found**: Ensure data files are in `/opt/resources/` inside the container
3. **Notebook import fails**: Check JSON syntax and try manual import

### Spark Configuration (if needed)
```python
spark = SparkSession.builder \
    .appName("Evapotranspiration_ML_Analysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```
