# Big Data Programming

This repository contains resources for the course work project in the module `Big Data Programming` in the MSc in Big Data Analytics.

## Task 2
Follow the steps below to deploy the HDFS in docker.

# Step 1 - Deploy HDFS
Run the following commands in your terminal.

1. cd task-2/map-reduce
2. ./gradlew clean build
3. cd ..
4. cp map-reduce/build/libs/map-reduce-1.0.0-SNAPSHOT.jar deployment/resources/
5. sudo docker compose -f deployment/docker-compose.yml up -d

# Step 2 - Copy the input files
Run the following command in your terminal to access the terminal in the hadoop namenode.
```
sudo docker exec -it namenode bash
```

Then execute the following commands.
1. hdfs dfs -mkdir -p /user/task/
2. hdfs dfs -put /opt/hadoop/resources/data /user/task/data
3. hdfs dfs -ls /user/task/data/

# Step 3 - Execute MapReduce task
Run the following command in the terminal of the namenode.

```
yarn jar /opt/hadoop/resources/map-reduce-1.0.0-SNAPSHOT.jar org.iit.mapreduce.DistrictMonthlyWeatherAnalysis /user/task/data/weatherData.csv /user/task/data/locationData.csv /user/task/output/DistrictMonthlyWeatherAnalysis
```

# Step 4 - Verify the output and write to the file system
Run the following command in the terminal of the namenode to verify the output.

1. hadoop fs -ls /user/task/output/DistrictMonthlyWeatherAnalysis
2. hadoop fs -cat /user/task/output/DistrictMonthlyWeatherAnalysis/part-r-00000
3. hadoop fsck /user/task/output/DistrictMonthlyWeatherAnalysis/part-r-00000 -files -blocks -locations

Run the following command to write the output data into a csv file.

```
(echo "District,Year-Month,Total_Precipitation_mm,Mean_Temperature_C" && \
 hadoop fs -cat /user/task/output/DistrictMonthlyWeatherAnalysis/part-r-00000 | \
 sed 's/\t/|/g' | \
 sed 's/ - /,/g' | \
 sed 's/|Total Precipitation: /,/g' | \
 sed 's/ mm, Mean Temperature: /,/g' | \
 sed 's/°C//g') > /opt/hadoop/resources/district_monthly_weather.csv
```


## Task 3
To be completed


## Task 4
To be completed

