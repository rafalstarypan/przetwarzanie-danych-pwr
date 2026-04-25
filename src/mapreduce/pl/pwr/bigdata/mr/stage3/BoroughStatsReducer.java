package pl.pwr.bigdata.mr.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BoroughStatsReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        List<Double> xs = new ArrayList<>();
        double sum = 0.0;
        double sumSq = 0.0;
        for (Text v : values) {
            double x = Double.parseDouble(v.toString());
            xs.add(x);
            sum += x;
            sumSq += x * x;
        }
        int n = xs.size();
        if (n == 0) return;

        double mean = sum / n;
        double variance = (sumSq / n) - (mean * mean);
        double stddev = variance > 0 ? Math.sqrt(variance) : 0.0;

        Collections.sort(xs);
        double q20 = quantile(xs, 0.20);
        double q40 = quantile(xs, 0.40);
        double q60 = quantile(xs, 0.60);
        double q80 = quantile(xs, 0.80);

        outValue.set(String.format("%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f",
                mean, stddev, q20, q40, q60, q80));
        context.write(key, outValue);
    }

    private static double quantile(List<Double> sorted, double q) {
        int n = sorted.size();
        if (n == 0) return 0.0;
        int idx = (int) Math.floor(q * (n - 1));
        if (idx < 0) idx = 0;
        if (idx >= n) idx = n - 1;
        return sorted.get(idx);
    }
}
