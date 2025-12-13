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
CREATE TABLE avg_evapotranspiration_by_season
STORED AS TEXTFILE
LOCATION '/user/task/output/hive/avg_evapotranspiration_by_season'
AS
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
