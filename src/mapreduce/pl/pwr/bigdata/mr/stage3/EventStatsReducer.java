package pl.pwr.bigdata.mr.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EventStatsReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        List<Long> xs = new ArrayList<>();
        for (Text v : values) xs.add(Long.parseLong(v.toString()));
        if (xs.isEmpty()) return;
        Collections.sort(xs);
        long p33 = quantile(xs, 0.33);
        long p66 = quantile(xs, 0.66);
        outValue.set(p33 + "\t" + p66);
        context.write(key, outValue);
    }

    private static long quantile(List<Long> sorted, double q) {
        int n = sorted.size();
        int idx = (int) Math.floor(q * (n - 1));
        if (idx < 0) idx = 0;
        if (idx >= n) idx = n - 1;
        return sorted.get(idx);
    }
}
