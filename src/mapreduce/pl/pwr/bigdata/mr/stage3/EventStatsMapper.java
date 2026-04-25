package pl.pwr.bigdata.mr.stage3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EventStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;
        String[] f = line.split("\t", -1);
        if (f.length < 9) return;

        String borough = f[1];
        String countStr = f[8];
        long count;
        try { count = Long.parseLong(countStr); } catch (NumberFormatException e) { return; }
        if (count <= 0) return;

        outKey.set(borough);
        outValue.set(String.valueOf(count));
        context.write(outKey, outValue);
    }
}
