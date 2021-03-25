package bdtc;

import bdtc.hadoop.Counter;
import bdtc.hadoop.MyMapper;
import bdtc.hadoop.MyReducer;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Log4j
public class MapReduceApplication extends Configured implements Tool{

    /**
     * Entry point for the application
     *
     * @param args Optional arguments: InputDirectory, OutputDirectory
     * @throws Exception when ToolRunner.run() fails
     */
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new MapReduceApplication(), args);
        System.exit(result);
    }

    /**
     * @param args command line arguments
     * @return 0 if job finished successfully else -1
     * @throws Exception when error occurred
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        if (args.length != 2) {
            log.error("Usage: InputFileOrDirectory OutputDirectory");
            return -1;
        }

        String hdfsInputFileOrDirectory = args[0];
        String hdfsOutputDirectory = args[1];

        Job job = Job.getInstance(configuration, "screen temperature");

        job.setJarByClass(MapReduceApplication.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(hdfsInputFileOrDirectory));
        FileOutputFormat.setOutputPath(job, new Path((hdfsOutputDirectory)));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        org.apache.hadoop.mapreduce.Counter counter = job.getCounters().findCounter(Counter.MALFORMED);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
        return 0;
    }
}
