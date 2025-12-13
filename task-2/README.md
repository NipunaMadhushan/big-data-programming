# Task 2

## Part 1 - Map Reduce
Follow the steps below to setup HDFS and process data using map reduce.

### Step 1 - Deploy HDFS
Run the following commands in your terminal.

1. cd task-2/part1-mapreduce/map-reduce
2. ./gradlew clean build
3. cd ..
4. cp map-reduce/build/libs/map-reduce-1.0.0-SNAPSHOT.jar deployment/resources/
5. sudo docker compose -f deployment/docker-compose.yml up -d

### Step 2 - Upload data to HDFS
Run the following command in your terminal to access the terminal in the hadoop namenode.
```
sudo docker exec -it namenode bash
```

Then execute the following commands.
1. hdfs dfs -mkdir -p /user/task/
2. hdfs dfs -put /opt/hadoop/resources/data /user/task/data
3. hdfs dfs -ls /user/task/data/

### Step 3 - Execute MapReduce task
Run the following command in the terminal of the namenode.

- Calculate the total precipitation and mean temperature
```
yarn jar /opt/hadoop/resources/map-reduce-1.0.0-SNAPSHOT.jar org.iit.mapreduce.DistrictMonthlyWeatherAnalysis /user/task/data/weatherData.csv /user/task/data/locationData.csv /user/task/output/DistrictMonthlyWeatherAnalysis
```

- Highest total precipitation
```
yarn jar /opt/hadoop/resources/map-reduce-1.0.0-SNAPSHOT.jar org.iit.mapreduce.HighestPrecipitationAnalysis /user/task/data/weatherData.csv /user/task/output/HighestPrecipitationAnalysis
```

### Step 4 - Verify the output and write to the file system
Run the following command in the terminal of the namenode to verify the output.

- Calculate the total precipitation and mean temperature
1. hadoop fs -ls /user/task/output/DistrictMonthlyWeatherAnalysis
2. hadoop fs -cat /user/task/output/DistrictMonthlyWeatherAnalysis/part-r-00000
3. hadoop fsck /user/task/output/DistrictMonthlyWeatherAnalysis/part-r-00000 -files -blocks -locations

- Highest total precipitation
1. hadoop fs -ls /user/task/output/HighestPrecipitationAnalysis
2. hadoop fs -cat /user/task/output/HighestPrecipitationAnalysis/part-r-00000
3. hadoop fsck /user/task/output/HighestPrecipitationAnalysis/part-r-00000 -files -blocks -locations

Run the following command to write the output data into a csv file (Optional).

- Calculate the total precipitation and mean temperature
```
(echo "District,Year-Month,Total_Precipitation_mm,Mean_Temperature_C" && \
 hadoop fs -cat /user/task/output/DistrictMonthlyWeatherAnalysis/part-r-00000 | \
 sed 's/\t/|/g' | \
 sed 's/ - /,/g' | \
 sed 's/|Total Precipitation: /,/g' | \
 sed 's/ mm, Mean Temperature: /,/g' | \
 sed 's/°C//g') > /opt/hadoop/resources/district_monthly_weather.csv
```

## Part 2 - Apache Hive

### Step 1 - Deploy HDFS and Apache Hive
Run the following commands in your terminal.

1. cd task-2/part2-apachehive
5. sudo docker compose -f deployment/docker-compose.yml up -d

Wait for services to be healthy (especially check namenode and hive-metastore-postgresql)
```
docker-compose logs -f hive-metastore-init
```

### Step 2 - Upload data to HDFS
Run the following command in your terminal to access the terminal in the hadoop namenode.
```
sudo docker exec -it namenode bash
```

Then execute the following commands.
1. Create Directories in HDFS
```
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /user/task/data
```

2. Upload the CSV files (if not already uploaded)
```
hdfs dfs -put /path/to/weatherData.csv /user/task/data/
hdfs dfs -put /path/to/locationData.csv /user/task/data/
```

3. Verify files are uploaded
```
hdfs dfs -ls /user/task/data/
```

### Step 3 - Create Hive tables
Run the following command in your terminal to access the terminal in the datanode.
```
sudo docker exec -it hive-server hive
```

Once in the Hive CLI, create the tables using the following SQL queries.

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS weather_analysis;
USE weather_analysis;

-- Create external table for location data
CREATE EXTERNAL TABLE IF NOT EXISTS locations (
    location_id INT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    utc_offset_seconds INT,
    timezone STRING,
    timezone_abbreviation STRING,
    city_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/task/data/location/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create external table for weather data
CREATE EXTERNAL TABLE IF NOT EXISTS weather_raw (
    location_id INT,
    date_str STRING,
    weather_code INT,
    temperature_2m_max DOUBLE,
    temperature_2m_min DOUBLE,
    temperature_2m_mean DOUBLE,
    apparent_temperature_max DOUBLE,
    apparent_temperature_min DOUBLE,
    apparent_temperature_mean DOUBLE,
    daylight_duration DOUBLE,
    sunshine_duration DOUBLE,
    precipitation_sum DOUBLE,
    rain_sum DOUBLE,
    precipitation_hours DOUBLE,
    wind_speed_10m_max DOUBLE,
    wind_gusts_10m_max DOUBLE,
    wind_direction_10m_dominant INT,
    shortwave_radiation_sum DOUBLE,
    et0_fao_evapotranspiration DOUBLE,
    sunrise STRING,
    sunset STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/task/data/weather/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Verify tables are created
SHOW TABLES;

-- Check sample data
SELECT * FROM locations LIMIT 5;
SELECT * FROM weather_raw LIMIT 5;
```

### Step 4 - Prepare data files in HDFS
Run the following command in your terminal to access the terminal in the hadoop namenode.
```
sudo docker exec -it namenode bash
```

In the HDFS CLI, run the following commands.

1. Create directories
```
hdfs dfs -mkdir -p /user/task/data/location
hdfs dfs -mkdir -p /user/task/data/weather
```

2. Copy files to appropriate directories
```
hdfs dfs -cp /user/task/data/locationData.csv /user/task/data/location/
hdfs dfs -cp /user/task/data/weatherData.csv /user/task/data/weather/
```

3. Verify data
```
hdfs dfs -ls /user/task/data/location/
hdfs dfs -ls /user/task/data/weather/
```

### Step 5 - Solve question 1 (Top 10 most temperate cities)
Run the following SQL queries in the Hive CLI.

```sql
-- Back in Hive CLI
USE weather_analysis;

-- Calculate average maximum temperature for each city
-- Lower temperature_2m_max means more temperate
SELECT 
    l.city_name,
    AVG(w.temperature_2m_max) as avg_max_temperature
FROM 
    weather_raw w
JOIN 
    locations l ON w.location_id = l.location_id
GROUP BY 
    l.city_name
ORDER BY 
    avg_max_temperature ASC
LIMIT 10;

-- Create a table to store results
CREATE TABLE top_10_temperate_cities AS
SELECT 
    l.city_name,
    ROUND(AVG(w.temperature_2m_max), 2) as avg_max_temperature
FROM 
    weather_raw w
JOIN 
    locations l ON w.location_id = l.location_id
GROUP BY 
    l.city_name
ORDER BY 
    avg_max_temperature ASC
LIMIT 10;

-- View results
SELECT * FROM top_10_temperate_cities;
```

### Step 6 - Solve question 2 (Average evapotranspiration by season)
Run the following SQL queries in the Hive CLI.

```sql
USE weather_analysis;

-- First, create a table with parsed dates and seasons
CREATE TABLE weather_with_season AS
SELECT 
    w.*,
    l.city_name,
    CAST(regexp_extract(w.date_str, '^(\\d+)/', 1) AS INT) as month,
    CAST(regexp_extract(w.date_str, '/(\\d+)$', 1) AS INT) as year,
    CASE 
        WHEN CAST(regexp_extract(w.date_str, '^(\\d+)/', 1) AS INT) IN (9, 10, 11, 12, 1, 2, 3) 
        THEN 'September-March'
        ELSE 'April-August'
    END as season
FROM 
    weather_raw w
JOIN 
    locations l ON w.location_id = l.location_id;

-- Calculate average evapotranspiration by district and season
CREATE TABLE avg_evapotranspiration_by_season AS
SELECT 
    city_name as district,
    season,
    ROUND(AVG(et0_fao_evapotranspiration), 2) as avg_evapotranspiration
FROM 
    weather_with_season
GROUP BY 
    city_name, season
ORDER BY 
    city_name, season;

-- View results
SELECT * FROM avg_evapotranspiration_by_season;
```

### Step 7 - Export results to CSV
In the Hive server container, run the following commands.

1. Export Question 1 results
```
hive -e "USE weather_analysis; SELECT * FROM top_10_temperate_cities;" > /tmp/top_10_temperate_cities.csv
```

2. Export Question 2 results
```
hive -e "USE weather_analysis; SELECT * FROM avg_evapotranspiration_by_season;" > /tmp/avg_evapotranspiration_by_season.csv
```

Then, run the following commands in your terminal to copy the data from docker container into your file system.
```
docker cp hive-server:/tmp/top_10_temperate_cities.csv ./
docker cp hive-server:/tmp/avg_evapotranspiration_by_season.csv ./
```

## Part 3 - Apache Spark
To be completed

