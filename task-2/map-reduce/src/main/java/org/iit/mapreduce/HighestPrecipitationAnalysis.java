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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HighestPrecipitationAnalysis {

    public static class PrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text outputKey = new Text();
        private DoubleWritable outputValue = new DoubleWritable();
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
                // date, location_id, precipitation_sum, ...
                String date = fields[0].trim();
                String precipitation = fields[2].trim();

                // Skip if precipitation is missing or empty
                if (precipitation.isEmpty() || date.isEmpty()) {
                    return;
                }

                // Extract year and month from date (Format: YYYY-MM-DD)
                String[] dateParts = date.split("-");
                if (dateParts.length < 2) {
                    return;
                }
                String year = dateParts[0];
                String month = dateParts[1];

                // Create composite key: year-month
                String compositeKey = year + "-" + month;
                outputKey.set(compositeKey);

                // Parse and emit precipitation value
                double precipitationValue = Double.parseDouble(precipitation);
                outputValue.set(precipitationValue);

                context.write(outputKey, outputValue);

            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                // Skip malformed records
                return;
            }
        }
    }

    public static class PrecipitationReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        private double maxPrecipitation = 0.0;
        private String maxMonth = "";
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalPrecipitation = 0.0;

            // Sum all precipitation values for this month
            for (DoubleWritable value : values) {
                totalPrecipitation += value.get();
            }

            // Check if this is the highest precipitation
            if (totalPrecipitation > maxPrecipitation) {
                maxPrecipitation = totalPrecipitation;
                maxMonth = key.toString();
            }

            // Write intermediate results (all month totals)
            String output = String.format("Total Precipitation: %.2f mm", totalPrecipitation);
            outputValue.set(output);
            context.write(key, outputValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Write the final result with the maximum precipitation
            if (!maxMonth.isEmpty()) {
                String[] dateParts = maxMonth.split("-");
                String year = dateParts[0];
                String month = dateParts[1];

                String keyString = "HIGHEST_PRECIPITATION";
                String valueString = String.format("Month: %s, Year: %s, Total Precipitation: %.2f mm",
                        month, year, maxPrecipitation);

                outputKey.set(keyString);
                outputValue.set(valueString);

                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: HighestPrecipitationAnalysis <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Highest Precipitation Analysis");
        job.setJarByClass(HighestPrecipitationAnalysis.class);
        
        job.setMapperClass(PrecipitationMapper.class);
        job.setReducerClass(PrecipitationReducer.class);

        // Use single reducer to find global maximum
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
