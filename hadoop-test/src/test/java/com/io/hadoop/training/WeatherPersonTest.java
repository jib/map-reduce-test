package com.io.hadoop.training;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Unit test for simple App.
 */
public class WeatherPersonTest {

    /**
     * Test to process weather records
     */
    @Test
    public void processWeatherRecords() throws IOException {
        WeatherPerson.WeatherPersonMR mapper = new WeatherPerson.WeatherPersonMR();

        Text inputValue = new Text("San Francisco^Sensor 1^Aug 25 01:00:00 PST 2012^55");
        OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

        mapper.map(null, inputValue, output, null);

        verify(output).collect(new Text("San Francisco^2012-08-25"), new IntWritable(55));
    }


    /**
     * Test invalid record
     */
    @Test
    public void processInvalidDateRecords() throws IOException {
        WeatherPerson.WeatherPersonMR mapper = new WeatherPerson.WeatherPersonMR();

        Text inputValue = new Text("San Francisco^Sensor 1^Aug 25:00:00 PST 2012^55");
        OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

        mapper.map(null, inputValue, output, null);

        verifyZeroInteractions(output);
    }
}
