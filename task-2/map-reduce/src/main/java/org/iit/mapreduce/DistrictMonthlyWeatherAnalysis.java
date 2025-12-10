/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.iit.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictMonthlyWeatherAnalysis {

    public static class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private boolean isFirstLine = true;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            
            if (line.isEmpty()) {
                return;
            }

            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            String[] fields = line.split(",");

            try {
                if (fields.length < 12) {
                    return;
                }

                String locationId = fields[0].trim();
                String dateStr = fields[1].trim();
                String temperature = fields[5].trim();
                String precipitation = fields[11].trim();

                if (dateStr.isEmpty() || locationId.isEmpty() || 
                    temperature.isEmpty() || precipitation.isEmpty() ||
                    temperature.equals("null") || precipitation.equals("null")) {
                    return;
                }

                // Parse date to get month only (not year-month)
                SimpleDateFormat inputFormat = new SimpleDateFormat("M/d/yyyy");
                Date date = inputFormat.parse(dateStr);
                
                SimpleDateFormat monthFormat = new SimpleDateFormat("MM");
                String month = monthFormat.format(date);

                // Create composite key: locationId-month (no year!)
                String compositeKey = locationId + "-" + month;
                outputKey.set(compositeKey);

                // Emit: key = "locationId-month", value = "precipitation,temperature"
                String compositeValue = precipitation + "," + temperature;
                outputValue.set(compositeValue);

                context.write(outputKey, outputValue);

            } catch (Exception e) {
                return;
            }
        }
    }

    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {

        private HashMap<String, String> locationMap = new HashMap<>();
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                File file = new File("locationData.csv");
                
                if (!file.exists()) {
                    System.err.println("ERROR: locationData.csv not found");
                    return;
                }

                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                int lineNum = 0;
                
                while ((line = reader.readLine()) != null) {
                    lineNum++;
                    
                    if (lineNum == 1) {
                        continue;
                    }

                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    String[] fields = line.split(",");
                    if (fields.length >= 8) {
                        String locationId = fields[0].trim();
                        String cityName = fields[7].trim();
                        
                        if (!locationId.isEmpty() && !cityName.isEmpty()) {
                            locationMap.put(locationId, cityName);
                        }
                    }
                }
                reader.close();
                
                System.out.println("INFO: Loaded " + locationMap.size() + " locations");
                
            } catch (Exception e) {
                System.err.println("ERROR loading location data: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double totalPrecipitation = 0.0;
            double totalTemperature = 0.0;
            int count = 0;

            // Aggregate all values for this district-month combination across all years
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                try {
                    if (parts.length >= 2) {
                        double precipitation = Double.parseDouble(parts[0]);
                        double temperature = Double.parseDouble(parts[1]);

                        totalPrecipitation += precipitation;
                        totalTemperature += temperature;
                        count++;
                    }
                } catch (NumberFormatException e) {
                    continue;
                }
            }

            if (count > 0) {
                double meanTemperature = totalTemperature / count;

                String[] keyParts = key.toString().split("-");
                if (keyParts.length >= 2) {
                    String locationId = keyParts[0];
                    String month = keyParts[1];

                    String districtName = locationMap.getOrDefault(locationId, "Location_" + locationId);

                    // Format output: District - Month, Total Precipitation: X mm, Mean Temperature: Y°C
                    String formattedKey = districtName + " - " + month;
                    String formattedValue = String.format("Total Precipitation: %.2f mm, Mean Temperature: %.2f°C",
                            totalPrecipitation, meanTemperature);

                    outputKey.set(formattedKey);
                    outputValue.set(formattedValue);

                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 3) {
            System.err.println("Usage: DistrictMonthlyWeatherAnalysis <weather_input> <location_file> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "District Monthly Weather Analysis");
        job.setJarByClass(DistrictMonthlyWeatherAnalysis.class);
        
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(otherArgs[1] + "#locationData.csv"));
        job.createSymlink();

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
**Expected Output Format:**
Gampaha - 01    Total Precipitation: 1250.50 mm, Mean Temperature: 25.30°C
Gampaha - 02    Total Precipitation: 890.20 mm, Mean Temperature: 26.10°C
Gampaha - 03    Total Precipitation: 1050.00 mm, Mean Temperature: 27.50°C
Colombo - 01    Total Precipitation: 1100.00 mm, Mean Temperature: 27.00°C
Colombo - 02    Total Precipitation: 950.50 mm, Mean Temperature: 28.20°C
*/
