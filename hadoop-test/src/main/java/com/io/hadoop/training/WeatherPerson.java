package com.io.hadoop.training;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Weather Person example
 * <p/>
 * This map reduce job reads a set of data sent by sensors (typically once per hour) and outputs the minimum
 * and maximum temparatures for each city.
 */
public class WeatherPerson extends Configured implements Tool {

    public static class WeatherPersonMR
            implements Mapper<LongWritable, Text, Text, IntWritable>,
            Reducer<Text, IntWritable, Text, NullWritable> {

        /**
         * Mappers/Reducers are passed the JobConf for the job via the configure method.
         *
         * @param conf Job Configuration
         */
        @Override
        public void configure(JobConf conf) {
            // Configure method
        }

        /**
         * This does cleanup after the mappers and reducers are executed.
         *
         * @throws IOException
         */
        @Override
        public void close() throws IOException {
            // Close method

        }

        /**
         * User defined Map method
         *
         * @param key      Key for the mapper.
         * @param row      Value for the mapper. This is the row that is read from the files.
         * @param output   Collects the output of the mapper
         * @param reporter Reports the progress
         * @throws IOException Exception if any..
         */
        @Override
        public void map(LongWritable key, Text row,
                        OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            // Data is in the files with the format:
            // City^sensorId^datetime^temperature
            String[] rowParts = row.toString().split("\\^");
            if (rowParts.length == 4) {
                // key: City^date
                // value: temperature

                // First convert the datetime to date.
                // Format of the date is: Aug 25 01:00:00 PST 2012
                SimpleDateFormat formatter = new SimpleDateFormat("MMM dd HH:mm:ss zzz yyyy");
                Date date;
                try {
                    date = formatter.parse(rowParts[2]);
                } catch (Exception ex) {
                    // Date could not be parsed, ignore it
                    date = null;
                }

                // Ensure that date in the input files are valid
                if (date != null) {
                    // Format the date as date without time.
                    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
                    String dateStr = dateFormatter.format(date);

                    // Parse the input temparatures as Integers
                    Integer temperature;
                    try {
                        temperature = Integer.parseInt(rowParts[3]);
                    } catch (Exception ex) {
                        // Temperature could not be parsed, ignore it
                        temperature = null;
                    }

                    if (temperature != null) {
                        output.collect(new Text(rowParts[0] + "^" + dateStr), new IntWritable(temperature));
                    }
                }
            }
        }

        /**
         * User defined Reducer method
         *
         * @param key      Key for the reducer
         * @param values   All the values for the key
         * @param output   Collects the output of the reducer
         * @param reporter Reports the progress
         * @throws IOException Exception if any..
         */
        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, NullWritable> output, Reporter reporter)
                throws IOException {

            // key: City^dateStr
            // values: temperature


            // initialize the min and max temperature
            int minTemperature = Integer.MAX_VALUE;
            int maxTemperature = Integer.MIN_VALUE;

            int numValues = 0;
            while (values.hasNext()) {
                numValues++;
                int temperature = values.next().get();

                if (temperature < minTemperature) {
                    minTemperature = temperature;
                }

                if (temperature > maxTemperature) {
                    maxTemperature = temperature;
                }
            }
            output.collect(new Text(key + "^" + minTemperature + "^" + maxTemperature), NullWritable.get());

            String[] rowParts = key.toString().split("\\^");

            reporter.incrCounter("Number of Weather Records by day", rowParts[1], numValues);
        }

    }

    public int run(String[] args) throws Exception {

        // JobConf is the interface for a user to describe a Map/Reduce job to the Hadoop framework for execution.
        JobConf conf = new JobConf(getConf(), WeatherPerson.class);
        conf.setJobName(" Weather Person Example");

        // Set Map Output Key and Value class
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        // Set the Reducer output key and value class
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);

        // Set the Mapper and Reducer class.
        // These can be different classes as long as they implement the map and reduce interfaces
        conf.setMapperClass(WeatherPersonMR.class);

        // Use conters here
        conf.setReducerClass(WeatherPersonMR.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WeatherPerson(), args);
        System.exit(ret);
    }


}
