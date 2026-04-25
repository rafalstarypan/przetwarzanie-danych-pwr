package pl.pwr.bigdata.mr.stage2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RidesAggDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: RidesAggDriver <input_dir> <output_dir> [num_reducers]");
            return 2;
        }
        int numReducers = args.length >= 3 ? Integer.parseInt(args[2]) : 1;

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "E2a-rides-agg");
        job.setJarByClass(RidesAggDriver.class);
        job.setMapperClass(RidesAggMapper.class);
        job.setCombinerClass(RidesAggCombiner.class);
        job.setReducerClass(RidesAggReducer.class);
        job.setNumReduceTasks(numReducers);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new RidesAggDriver(), args));
    }
}
