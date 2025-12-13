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
Follow the steps below to setup HDFS and Apache Hive.

### Step 1 - Deploy HDFS and Apache Hive
Run the following commands in your terminal.

1. cd task-2/part2-apachehive
2. sudo docker compose -f deployment/docker-compose.yml up -d

Wait for services to be healthy (especially check namenode and hive-metastore-postgresql)
```
sudo docker logs -f hive-metastore-init
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
hdfs dfs -put /opt/resources/data/weatherData.csv /user/task/data/
hdfs dfs -put /opt/resources/data/locationData.csv /user/task/data/
```

3. Verify files are uploaded
```
hdfs dfs -ls /user/task/data/
```

### Step 3 - Create Hive tables
Run the following command in your terminal.
```
sudo docker exec -it hive-server hive -f ./resources/scripts/create-hive-tables.hql
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

### Step 5 - Analyze data
Run the following command in your terminal to get `top 10 most temperate cities`.
```
sudo docker exec -it hive-server hive -f ./resources/scripts/top-10-most-temperate-cities.hql
```

Run the following command in your terminal to get `average evapotranspiration by season`.
```
sudo docker exec -it hive-server hive -f ./resources/scripts/average-evapotranspiration-by-season.hql
```

### Step 6 - Verify the output tables
Run the following command in the namenode terminal.

1. Check whether the tables have been created
```
hdfs dfs -ls /user/task/output/hive/
```

2. Check the table data
```
hdfs dfs -cat /user/task/output/hive/top_10_temperate_cities/000000_0
hdfs dfs -cat /user/task/output/hive/avg_evapotranspiration_by_season/000000_0
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
Follow the steps below to setup HDFS and Apache Spark.

### Step 1 - Deploy HDFS and Apache Spark
Run the following commands in your terminal.

1. cd task-2/part3-apachespark
2. sudo docker compose -f deployment/docker-compose.yml up -d


### Step 2 - Upload data to HDFS
Run the following command in your terminal to access the terminal in the hadoop namenode.
```
sudo docker exec -it namenode bash
```

Then execute the following commands.
1. Create Directories in HDFS
```
hdfs dfs -mkdir -p /user/task/data
```

2. Upload the CSV files (if not already uploaded)
```
hdfs dfs -put /opt/resources/data/weatherData.csv /user/task/data/
hdfs dfs -put /opt/resources/data/locationData.csv /user/task/data/
```

3. Verify files are uploaded
```
hdfs dfs -ls /user/task/data/
```

### Step 3 - Analyze data
Run the following command in your terminal to get the `percentage of total shortwave radiation`.
```
sudo docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/resources/scripts/q1_shortwave_radiation.py
```

Run the following command in your terminal to get the `weekly maximum temperatures for the hottest months of an year`.
```
sudo docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/resources/scripts/q2_weekly_max_temp.py
```

### Step 4 - View results
Run the following commands in the namenode terminal.

```
docker exec namenode hdfs dfs -cat /user/task/output/spark/q1_shortwave_radiation/*.csv | head -20
docker exec namenode hdfs dfs -cat /user/task/output/spark/q2_weekly_max_temp/*.csv | head -20
```

### Step 5 - Get results to the file system
Run the following commands in your terminal.

```
docker exec namenode hdfs dfs -getmerge /user/task/output/spark/q1_shortwave_radiation ./q1_shortwave_radiation.csv
docker exec namenode hdfs dfs -getmerge /user/task/output/spark/q2_weekly_max_temp ./q2_weekly_max_temp.csv
```
