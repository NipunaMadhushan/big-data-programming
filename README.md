# Big Data Programming

This repository contains resources for the course work project in the module `Big Data Programming` in the MSc in Big Data Analytics.

## Task 2
Follow the steps below to deploy the HDFS in docker.

1. cd task-2/map-reduce
2. ./gradlew clean build
3. cd ..
3. cp map-reduce/build/libs/map-reduce-1.0.0-SNAPSHOT.jar deployment/resources/
4. sudo docker compose -f deployment/docker-compose.yml up -d


1. sudo docker exec -it namenode bash
2. hdfs dfs -mkdir -p /user/task/
3. hdfs dfs -put /opt/hadoop/resources/data /user/task/data
4. hdfs dfs -ls /user/task/data/


yarn jar /opt/hadoop/resources/map-reduce-1.0.0-SNAPSHOT.jar org.iit.mapreduce.DistrictMonthlyWeatherAnalysis /user/task/data/ /user/task/output/DistrictMonthlyWeatherAnalysis



## Task 3
To be completed


## Task 4
To be completed

