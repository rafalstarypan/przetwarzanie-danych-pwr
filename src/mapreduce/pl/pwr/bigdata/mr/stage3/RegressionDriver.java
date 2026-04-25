package pl.pwr.bigdata.mr.stage3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RegressionDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: RegressionDriver <t2_dir> <output_dir> [no-combiner]");
            return 2;
        }
        boolean useCombiner = !(args.length >= 3 && "no-combiner".equalsIgnoreCase(args[2]));

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "E3b-regression" + (useCombiner ? "" : "-nocomb"));
        job.setJarByClass(RegressionDriver.class);
        job.setMapperClass(RegressionMapper.class);
        if (useCombiner) job.setCombinerClass(RegressionCombiner.class);
        job.setReducerClass(RegressionReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new RegressionDriver(), args));
    }
}
