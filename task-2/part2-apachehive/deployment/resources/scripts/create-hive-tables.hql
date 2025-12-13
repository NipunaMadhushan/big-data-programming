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
