USE weather_analysis;

DROP TABLE IF EXISTS top_10_temperate_cities;

-- Calculate average maximum temperature for each city
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
CREATE TABLE top_10_temperate_cities
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/task/output/hive/top_10_temperate_cities'
AS
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
