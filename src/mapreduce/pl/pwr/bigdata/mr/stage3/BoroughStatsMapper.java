package pl.pwr.bigdata.mr.stage3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BoroughStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;
        String[] f = line.split("\t", -1);
        if (f.length < 3) return;
        outKey.set(f[1]);
        outValue.set(f[2]);
        context.write(outKey, outValue);
    }
}
