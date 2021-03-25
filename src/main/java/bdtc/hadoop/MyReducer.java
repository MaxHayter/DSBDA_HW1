package bdtc.hadoop;

import bdtc.utils.Temperature;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;

/**
 * Reducer class
 * Input key {@link Text}
 * Input value {@link IntWritable}
 * Output key {@link Text}
 * Output value {@link Text}
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, Text> {

    /**
     * Converter: number of clicks -> temperature
     */
    private static Temperature temperature;

    /**
     * Initial reducer setup
     * @param context reducer context
     * @throws IOException in Temperature constructor
     */
    @Override
    protected void setup(Context context) throws IOException {
        URI[] uris = context.getCacheFiles();
        for (URI uri : uris) {
            if (uri.getPath().contains("temperature")) {
                temperature = new Temperature(context.getConfiguration(), new Path(uri.getPath()));
                break;
            }
        }
    }

    /**
     * Reduce function. Counts the number of clicks and converts it to temperature
     * @param key Text
     * @param values iterable of values
     * @param context reducer context
     * @throws IOException in context.write()
     * @throws InterruptedException in context.write()
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
        }
        context.write(key, new Text(temperature.getTemperatureByNum(sum)));
    }
}
