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
        private int lineCount = 0;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            lineCount++;
            
            // Skip header line (first line)
            if (lineCount == 1) {
                return;
            }

            String line = value.toString().trim();
            
            // Skip empty lines
            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split(",");

            try {
                // Print first record for debugging (only once)
                if (lineCount == 2) {
                    System.out.println("DEBUG: First data record has " + fields.length + " fields");
                    System.out.println("DEBUG: Sample record: " + line.substring(0, Math.min(200, line.length())));
                }

                // Extract fields - adjust indices based on your CSV structure
                // Common structure: date, location_id, precipitation_sum, temperature_2m_mean, ...
                if (fields.length < 4) {
                    return; // Skip records with insufficient fields
                }

                String date = fields[0].trim();
                String locationId = fields[1].trim();
                String precipitation = fields[2].trim();
                String temperature = fields[3].trim();

                // Skip if essential fields are missing or empty
                if (date.isEmpty() || locationId.isEmpty() || 
                    precipitation.isEmpty() || temperature.isEmpty() ||
                    precipitation.equals("null") || temperature.equals("null")) {
                    return;
                }

                // Extract year and month from date (Format: YYYY-MM-DD)
                String[] dateParts = date.split("-");
                if (dateParts.length < 2) {
                    return;
                }
                String year = dateParts[0];
                String month = dateParts[1];

                // Validate year and month
                if (year.length() != 4 || month.length() > 2) {
                    return;
                }

                // Create composite key: locationId-year-month
                String compositeKey = locationId + "-" + year + "-" + month;
                outputKey.set(compositeKey);

                // Emit: key = "locationId-year-month", value = "precipitation,temperature"
                String compositeValue = precipitation + "," + temperature;
                outputValue.set(compositeValue);

                context.write(outputKey, outputValue);

            } catch (Exception e) {
                // Skip malformed records silently
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
                // Load location data from distributed cache
                // The file will be in the current working directory with the symlink name
                File file = new File("locationData.csv");
                
                if (!file.exists()) {
                    System.err.println("ERROR: locationData.csv not found in working directory");
                    return;
                }

                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                int lineNum = 0;
                
                while ((line = reader.readLine()) != null) {
                    lineNum++;
                    
                    // Skip header
                    if (lineNum == 1) {
                        continue;
                    }

                    // Skip empty lines
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    String[] fields = line.split(",");
                    if (fields.length >= 2) {
                        String locationId = fields[0].trim();
                        String cityName = fields[1].trim();
                        
                        if (!locationId.isEmpty() && !cityName.isEmpty()) {
                            locationMap.put(locationId, cityName);
                        }
                    }
                }
                reader.close();
                
                System.out.println("INFO: Loaded " + locationMap.size() + " locations from cache file");
                
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

            // Aggregate values
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
                    // Skip invalid values
                    continue;
                }
            }

            // Calculate mean temperature
            if (count > 0) {
                double meanTemperature = totalTemperature / count;

                // Parse the composite key to extract location, year, and month
                String[] keyParts = key.toString().split("-");
                if (keyParts.length >= 3) {
                    String locationId = keyParts[0];
                    String year = keyParts[1];
                    String month = keyParts[2];

                    // Get district name from location map
                    String districtName = locationMap.getOrDefault(locationId, "Location_" + locationId);

                    // Format output key and value
                    String formattedKey = districtName + " - " + year + "-" + month;
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

        // Add location file to distributed cache with symlink
        job.addCacheFile(new URI(otherArgs[1] + "#locationData.csv"));
        
        // Create symlink for distributed cache
        job.createSymlink();

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
