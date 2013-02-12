package com.io.hadoop.training;

import java.io.IOException;
import java.util.*;

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


import org.apache.commons.lang.StringUtils;

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

            // Following: http://wiki.apache.org/hadoop/WordCount
            String line = row.toString();

            //System.out.println( "Input: " + line );

            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                Text word   = new Text( tokenizer.nextToken( ) );

                // Following: http://stackoverflow.com/questions/605891/sort-a-single-string-in-java
                char[] chars = word.toString().toCharArray();
                Arrays.sort( chars );

                // Hadoop insists on using the Text type for input/output, not string
                Text sorted = new Text( new String(chars) );

                //System.out.println( "Word: " + word );
                //System.out.println( "Key: " + sorted );

                output.collect( sorted, word );
            }



        }

        public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, NullWritable> output, Reporter reporter)
                throws IOException {
            // Key: ascii-sorted word
            // Value: words

            // Arrays are a fixed size.. Since we don't know the amount of values (nor should we care), it's
            // not suitable. There's a List implementation that does grow, but it's apparently not part of the
            // standard library..
            // String[] words;
            // Instead, use ArrayList, which can be extended:
            // http://docs.oracle.com/javase/6/docs/api/java/util/ArrayList.html
            List<String> words = new ArrayList<String>();



            // So.. Java doesn't have an Array.join() method!?
            // http://stackoverflow.com/questions/1978933/a-quick-and-easy-way-to-join-array-elements-with-a-separator-the-oposite-of-spl
            // Write a ghetto version ourselves instead then...
            int _seenFirstElement = 0;

            while (iterator.hasNext()) {

                // Only skip this on the first element we're adding, meaning all subsequent invocations
                // add the '^' first, then their word.
                // Also, ++_seenFirstElement doesn't work either.. and this HAS to be a boolean, not an int...
                // DWIM already...

                words.add( iterator.next().toString() );
            }


            output.collect(new Text( StringUtils.join(words, "^") ), NullWritable.get());
        }
    }

}