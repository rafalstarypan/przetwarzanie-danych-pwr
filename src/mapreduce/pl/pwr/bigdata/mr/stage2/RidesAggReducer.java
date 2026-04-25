package pl.pwr.bigdata.mr.stage2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RidesAggReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();
    private final boolean emitFinal;

    public RidesAggReducer() {
        this(true);
    }

    protected RidesAggReducer(boolean emitFinal) {
        this.emitFinal = emitFinal;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        long n = 0;
        long nMember = 0;
        double sumDur = 0.0;

        for (Text v : values) {
            String s = v.toString();
            if (s.startsWith("PARTIAL|")) {
                String[] p = s.split("\\|", -1);
                long pn = Long.parseLong(p[1]);
                long pm = Long.parseLong(p[2]);
                double psd = Double.parseDouble(p[3]);
                n += pn; nMember += pm; sumDur += psd;
            } else {
                String[] p = s.split("\\|", -1);
                String mc = p[1];
                double dur = Double.parseDouble(p[2]);
                n += 1;
                if ("member".equals(mc)) nMember += 1;
                sumDur += dur;
            }
        }

        if (emitFinal) {
            String[] kp = key.toString().split("\\|", 2);
            String date = kp[0];
            String borough = kp.length > 1 ? kp[1] : "";
            double avgDur = n > 0 ? sumDur / n : 0.0;
            outValue.set(date + "\t" + borough + "\t" + n + "\t" + nMember + "\t" + String.format("%.2f", avgDur));
            context.write(null, outValue);
        } else {
            outValue.set("PARTIAL|" + n + "|" + nMember + "|" + String.format("%.4f", sumDur));
            context.write(key, outValue);
        }
    }
}
