package pl.pwr.bigdata.mr.stage3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegressionMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;
        String[] f = line.split("\t", -1);
        if (f.length < 8) return;

        String borough = f[1];
        String total = f[2];
        String tempStr = f[5];
        String precipStr = f[6];
        String snowStr = f[7];

        if (tempStr.isEmpty()) {
            context.getCounter("E3b", "skipped_no_weather").increment(1);
            return;
        }

        outKey.set(borough);
        outValue.set("RAW|" + tempStr + "|" + precipStr + "|" + snowStr + "|" + total);
        context.write(outKey, outValue);
    }
}
