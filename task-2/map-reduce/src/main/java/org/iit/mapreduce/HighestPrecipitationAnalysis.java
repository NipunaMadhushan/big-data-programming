/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.iit.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
        private boolean isFirstLine = true;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            
            // Skip empty lines
            if (line.isEmpty()) {
                return;
            }

            // Check if this is the header line
            if (isFirstLine) {
                isFirstLine = false;
                return; // Skip header
            }

            String[] fields = line.split(",");

            try {
                // CSV Structure:
                // 0: location_id
                // 1: date (M/D/YYYY)
                // 11: precipitation_sum (mm)
                
                if (fields.length < 12) {
                    return;
                }

                String dateStr = fields[1].trim();
                String precipitation = fields[11].trim();

                // Skip if fields are missing or empty
                if (dateStr.isEmpty() || precipitation.isEmpty() || precipitation.equals("null")) {
                    return;
                }

                // Parse date from M/D/YYYY format
                SimpleDateFormat inputFormat = new SimpleDateFormat("M/d/yyyy");
                Date date = inputFormat.parse(dateStr);
                
                SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
                SimpleDateFormat monthFormat = new SimpleDateFormat("MM");
                
                String year = yearFormat.format(date);
                String month = monthFormat.format(date);

                // Create composite key: year-month
                String compositeKey = year + "-" + month;
                outputKey.set(compositeKey);

                // Parse and emit precipitation value
                double precipitationValue = Double.parseDouble(precipitation);
                outputValue.set(precipitationValue);

                context.write(outputKey, outputValue);

            } catch (Exception e) {
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
            String output = String.format("Total Precipitation: %.2f hours", totalPrecipitation);
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
                String valueString = String.format("Month: %s, Year: %s, Total Precipitation: %.2f hours",
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
