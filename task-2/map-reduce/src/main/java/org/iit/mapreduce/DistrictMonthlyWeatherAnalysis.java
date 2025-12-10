/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.iit.mapreduce;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class DistrictMonthlyWeatherAnalysis {

    public static class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private boolean isHeader = true;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Skip header line
            if (isHeader) {
                isHeader = false;
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",");

            try {
                // Assuming CSV structure:
                // date, location_id, precipitation_sum, temperature_2m_mean, ...
                String date = fields[0].trim();
                String locationId = fields[1].trim();
                String precipitation = fields[2].trim();
                String temperature = fields[3].trim();

                // Skip if essential fields are missing or empty
                if (precipitation.isEmpty() || temperature.isEmpty() ||
                        date.isEmpty() || locationId.isEmpty()) {
                    return;
                }

                // Extract year and month from date (Format: YYYY-MM-DD)
                String[] dateParts = date.split("-");
                if (dateParts.length < 2) {
                    return;
                }
                String year = dateParts[0];
                String month = dateParts[1];

                // Create composite key: locationId-year-month
                String compositeKey = locationId + "-" + year + "-" + month;
                outputKey.set(compositeKey);

                // Emit: key = "locationId-year-month", value = "precipitation,temperature"
                String compositeValue = precipitation + "," + temperature;
                outputValue.set(compositeValue);

                context.write(outputKey, outputValue);

            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                // Skip malformed records
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
            // Load location data from distributed cache
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(
                        new FileReader(cacheFiles[0].getPath()));

                String line;
                boolean firstLine = true;

                while ((line = reader.readLine()) != null) {
                    if (firstLine) {
                        firstLine = false;
                        continue; // Skip header
                    }

                    String[] fields = line.split(",");
                    if (fields.length >= 2) {
                        String locationId = fields[0].trim();
                        String cityName = fields[1].trim();
                        locationMap.put(locationId, cityName);
                    }
                }
                reader.close();
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
                    double precipitation = Double.parseDouble(parts[0]);
                    double temperature = Double.parseDouble(parts[1]);

                    totalPrecipitation += precipitation;
                    totalTemperature += temperature;
                    count++;
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
                String locationId = keyParts[0];
                String year = keyParts[1];
                String month = keyParts[2];

                // Get district name from location map
                String districtName = locationMap.getOrDefault(locationId, "Unknown_" + locationId);

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

        // Add location file to distributed cache
        job.addCacheFile(new URI(otherArgs[1]));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
