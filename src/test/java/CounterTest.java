import bdtc.hadoop.Counter;
import bdtc.hadoop.MyMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CounterTest {
    private final String validRow = "673,1000 16 - 2020-11-25 23:49:11";
    private final String malformed= "4487729736509862376,,Ww== - 16041466168-04-08 21:30:00 +0300 MSK";

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setup(){
        MyMapper mapper = new MyMapper();
        mapDriver = new MapDriver<>(mapper);
        mapDriver.addCacheFile(new File("src/test/resources/screen_test").getAbsolutePath());
    }

    @Test
    public void testZeroCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(validRow))
                .withOutput(new Text("(0 , 1)"), new IntWritable(1))
                .runTest();

        assertEquals("MALFORMED COUNTER", 0,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED).getValue());
    }

    @Test
    public void testMalformedCounter() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(malformed))
                .runTest();
        assertEquals("MALFORMED COUNTER", 1,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED).getValue());
    }

    @Test
    public void testCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(validRow))
                .withInput(new LongWritable(), new Text(malformed))
                .withInput(new LongWritable(), new Text(malformed))
                .withOutput(new Text("(0 , 1)"), new IntWritable(1))
                .runTest();
        assertEquals("MALFORMED COUNTER", 2,  mapDriver.getCounters()
                .findCounter(Counter.MALFORMED).getValue());
    }
}