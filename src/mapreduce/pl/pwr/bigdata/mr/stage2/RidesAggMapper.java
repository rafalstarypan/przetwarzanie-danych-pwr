package pl.pwr.bigdata.mr.stage2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RidesAggMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;

        String[] f = line.split("\t", -1);
        if (f.length < 7) return;

        String date = f[1];
        String borough = f[2];
        String memberCasual = f[4];
        String durStr = f[5];

        outKey.set(date + "|" + borough);
        outValue.set("RAW|" + memberCasual + "|" + durStr);
        context.write(outKey, outValue);
    }
}
