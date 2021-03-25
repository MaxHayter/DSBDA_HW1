import bdtc.hadoop.MyMapper;
import bdtc.hadoop.MyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {
    private final String validRow1 = "349,49 91 - 2021-03-11 23:21:09";

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;


    @Before
    public void setup(){
        MyMapper mapper = new MyMapper();
        mapDriver = new MapDriver<>(mapper);
        mapDriver.addCacheFile(new File("src/test/resources/screen_test").getAbsolutePath());

        MyReducer reducer = new MyReducer();
        reduceDriver = new ReduceDriver<>(reducer);
        reduceDriver.addCacheFile(new File("src/test/resources/temperature_test").getAbsolutePath());

        mapReduceDriver = new MapReduceDriver<>(mapper, reducer);
        mapReduceDriver.addCacheFile(new File("src/test/resources/screen_test").getAbsolutePath());
        mapReduceDriver.addCacheFile(new File("src/test/resources/temperature_test").getAbsolutePath());
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(validRow1))
                .withOutput(new Text("(0 , 0)"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> iterable = new ArrayList<>();
        iterable.add(new IntWritable(3));
        iterable.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text("(1 , 0)"), iterable)
                .withOutput(new Text("(1 , 0)"), new Text("hot"))
                .runTest();
    }

    @Test
    public void testMapperAndReducer() throws IOException {
        String validRow2 = "740,322 21 - 2021-03-14 15:46:19";
        String malformed1 = "1752742903139208989,,Qw== - 191161279042-12-03 13:33:56 +0300 MSK";
        String malformed2 = "3752873402510354162,,FA== - 232390144651-07-19 21:10:59 +0300 MSK";
        mapReduceDriver
                .withInput(new LongWritable(), new Text(validRow1))
                .withInput(new LongWritable(), new Text(malformed1))
                .withInput(new LongWritable(), new Text(validRow2))
                .withInput(new LongWritable(), new Text(malformed2))
                .withOutput(new Text("(0 , 0)"), new Text("medium"))
                .runTest();
    }
}