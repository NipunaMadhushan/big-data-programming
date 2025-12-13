USE weather_analysis;

-- Calculate average maximum temperature for each city
SELECT 
    l.city_name,
    AVG(w.temperature_2m_mean) as avg_mean_temperature
FROM 
    weather_raw w
JOIN 
    locations l ON w.location_id = l.location_id
GROUP BY 
    l.city_name
ORDER BY 
    avg_mean_temperature ASC
LIMIT 10;

-- Create a table to store results
CREATE TABLE top_10_temperate_cities AS
SELECT 
    l.city_name,
    ROUND(AVG(w.temperature_2m_mean), 2) as avg_mean_temperature
FROM 
    weather_raw w
JOIN 
    locations l ON w.location_id = l.location_id
GROUP BY 
    l.city_name
ORDER BY 
    avg_mean_temperature DESC
LIMIT 10;

-- View results
SELECT * FROM top_10_temperate_cities;
