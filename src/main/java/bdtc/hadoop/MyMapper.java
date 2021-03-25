package bdtc.hadoop;

import bdtc.utils.Pair;
import bdtc.utils.Screen;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

/**
 * Mapper class
 * Input key {@link LongWritable}
 * Input value {@link Text}
 * Output key {@link Text}
 * Output value {@link Text}
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * Converter: screen coordinates -> name of the screen area
     */
    private static Screen screen;

    /**
     * Click confirmation
     */
    private final static IntWritable one = new IntWritable(1);

    /**
     * Regex pattern to check if the input row is correct
     */
    private final static Pattern reg = Pattern.compile("^[\\d]+,[\\d]+ [\\d]+ - [\\d]{4}-[\\d]{2}-[\\d]{2} [\\d]{2}:[\\d]{2}:[\\d]{2}$");

    /**
     * Regex pattern to split input row
     */
    private final static Pattern splitStr = Pattern.compile("[,\\- ]+");

    /**
     * Initial mapper setup
     * @param context mapper context
     * @throws IOException in Screen constructor
     */
    @Override
    protected void setup(Context context) throws IOException {
        URI[] uris = context.getCacheFiles();
        for (URI uri : uris) {
            if (uri.getPath().contains("screen")) {
                screen = new Screen(context.getConfiguration(), new Path(uri.getPath()));
                break;
            }
        }
    }

    /**
     * Map function. Checks data for correctness and determining the area that was clicked on
     * Uses counters {@link Counter}
     * @param key input key
     * @param value input value
     * @param context mapper context
     * @throws IOException from context.write()
     * @throws InterruptedException from context.write()
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str  = value.toString();
        if (reg.matcher(str).matches()) {
            String [] data = splitStr.split(str);
            try {
                String area = screen.getAreaByCoordinates(new Pair(Integer.parseInt(data[0]), Integer.parseInt(data[1])));
                context.write(new Text(area), one);
            }
            catch (RuntimeException error){
                context.getCounter(Counter.MALFORMED).increment(1);
            }
        }
        else {
            context.getCounter(Counter.MALFORMED).increment(1);
        }
    }
}

