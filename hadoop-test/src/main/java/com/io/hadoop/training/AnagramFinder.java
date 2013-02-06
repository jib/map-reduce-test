package com.io.hadoop.training;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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



public class AnagramFinder extends Configured implements Tool {

    public static void main(String[] args)
            throws Exception {
        int ret = ToolRunner.run(new AnagramFinder(), args);
        System.exit(ret);
    }

    public int run(String[] args)
            throws Exception {
        // JobConf is the interface for a user to describe a Map/Reduce job to the Hadoop framework for execution.
        JobConf conf = new JobConf(getConf(), AnagramFinderMR.class);
        conf.setJobName("Anagram Finder Example");

        // Set Map Output Key and Value class
        conf.setMapOutputKeyClass(  Text.class);
        conf.setMapOutputValueClass(Text.class);

        // Set the Reducer output key and value class
        conf.setOutputKeyClass(  Text.class);
        conf.setOutputValueClass(NullWritable.class);

        // Set the Mapper and Reducer class.
        // These can be different classes as long as they implement the map and reduce interfaces
        conf.setMapperClass(    AnagramFinderMR.class);
        conf.setReducerClass(   AnagramFinderMR.class);

        conf.setInputFormat( TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths( conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public  static  class   AnagramFinderMR
            implements  Mapper<LongWritable ,Text ,Text ,Text>
            ,Reducer<Text ,Text  ,Text ,NullWritable> {
        public void configure(JobConf jc) {

        }

        public void close()
                throws IOException {

        }


        public void map(LongWritable key, Text row, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            // TODO IMPLEMENT THIS
        }

        public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, NullWritable> output, Reporter reporter)
                throws IOException {

            // TODO IMPLEMENT THIS
        }
    }

}