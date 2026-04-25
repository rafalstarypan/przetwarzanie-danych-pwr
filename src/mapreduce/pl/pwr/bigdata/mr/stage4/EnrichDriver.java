package pl.pwr.bigdata.mr.stage4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class EnrichDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: EnrichDriver <t2_dir> <output_dir> <t3a_dir> <t3b_dir> <t3c_dir>");
            return 2;
        }
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "E4-enrich");
        job.setJarByClass(EnrichDriver.class);
        job.setMapperClass(EnrichMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        addCacheParts(job, fs, new Path(args[2]), "t3a_");
        addCacheParts(job, fs, new Path(args[3]), "t3b_");
        addCacheParts(job, fs, new Path(args[4]), "t3c_");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void addCacheParts(Job job, FileSystem fs, Path p, String aliasPrefix) throws java.io.IOException, java.net.URISyntaxException {
        if (!fs.exists(p)) {
            System.err.println("WARN: cache path missing: " + p);
            return;
        }
        FileStatus[] children = fs.listStatus(p);
        int idx = 0;
        for (FileStatus s : children) {
            if (s.isDirectory()) continue;
            String name = s.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;
            String alias = aliasPrefix + (idx++) + "_" + name;
            URI uri = new URI(s.getPath().toUri().toString() + "#" + alias);
            job.addCacheFile(uri);
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new EnrichDriver(), args));
    }
}
