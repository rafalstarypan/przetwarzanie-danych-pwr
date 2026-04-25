package pl.pwr.bigdata.mr.stage3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pl.pwr.bigdata.mr.common.LinearAlgebra;

public class RegressionReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();
    private final boolean emitFinal;

    public RegressionReducer() {
        this(true);
    }

    protected RegressionReducer(boolean emitFinal) {
        this.emitFinal = emitFinal;
    }

    static class Acc {
        double n;
        double sx1, sx2, sx3;
        double sx1sq, sx2sq, sx3sq;
        double sx1x2, sx1x3, sx2x3;
        double sy, sx1y, sx2y, sx3y;
        double sysq;

        void addRaw(double x1, double x2, double x3, double y) {
            n += 1;
            sx1 += x1; sx2 += x2; sx3 += x3;
            sx1sq += x1 * x1; sx2sq += x2 * x2; sx3sq += x3 * x3;
            sx1x2 += x1 * x2; sx1x3 += x1 * x3; sx2x3 += x2 * x3;
            sy += y; sx1y += x1 * y; sx2y += x2 * y; sx3y += x3 * y;
            sysq += y * y;
        }

        void addPartial(double[] p) {
            n += p[0];
            sx1 += p[1]; sx2 += p[2]; sx3 += p[3];
            sx1sq += p[4]; sx2sq += p[5]; sx3sq += p[6];
            sx1x2 += p[7]; sx1x3 += p[8]; sx2x3 += p[9];
            sy += p[10]; sx1y += p[11]; sx2y += p[12]; sx3y += p[13];
            sysq += p[14];
        }

        String serialize() {
            return "PARTIAL|" + n + "|" + sx1 + "|" + sx2 + "|" + sx3
                    + "|" + sx1sq + "|" + sx2sq + "|" + sx3sq
                    + "|" + sx1x2 + "|" + sx1x3 + "|" + sx2x3
                    + "|" + sy + "|" + sx1y + "|" + sx2y + "|" + sx3y
                    + "|" + sysq;
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        Acc a = new Acc();
        for (Text v : values) {
            String s = v.toString();
            if (s.startsWith("PARTIAL|")) {
                String[] p = s.split("\\|", -1);
                double[] arr = new double[15];
                for (int i = 0; i < 15; i++) arr[i] = Double.parseDouble(p[i + 1]);
                a.addPartial(arr);
            } else {
                String[] p = s.split("\\|", -1);
                double x1 = Double.parseDouble(p[1]);
                double x2 = Double.parseDouble(p[2]);
                double x3 = Double.parseDouble(p[3]);
                double y = Double.parseDouble(p[4]);
                a.addRaw(x1, x2, x3, y);
            }
        }

        if (!emitFinal) {
            outValue.set(a.serialize());
            context.write(key, outValue);
            return;
        }

        if (a.n < 4) {
            context.getCounter("E3b", "fallback_too_few_samples").increment(1);
            double mean = a.n > 0 ? a.sy / a.n : 0.0;
            outValue.set(String.format("%.2f\t0.00\t0.00\t0.00\t0.00", mean));
            context.write(key, outValue);
            return;
        }

        double[][] xtx = {
                { a.n,    a.sx1,   a.sx2,   a.sx3 },
                { a.sx1,  a.sx1sq, a.sx1x2, a.sx1x3 },
                { a.sx2,  a.sx1x2, a.sx2sq, a.sx2x3 },
                { a.sx3,  a.sx1x3, a.sx2x3, a.sx3sq }
        };
        double[] xty = { a.sy, a.sx1y, a.sx2y, a.sx3y };

        double[] beta;
        try {
            beta = LinearAlgebra.solve(xtx, xty);
        } catch (LinearAlgebra.SingularMatrixException e) {
            context.getCounter("E3b", "fallback_singular").increment(1);
            double mean = a.sy / a.n;
            outValue.set(String.format("%.2f\t0.00\t0.00\t0.00\t0.00", mean));
            context.write(key, outValue);
            return;
        }

        double rss = a.sysq
                - (beta[0] * a.sy + beta[1] * a.sx1y + beta[2] * a.sx2y + beta[3] * a.sx3y);
        if (rss < 0) rss = 0;
        double rmse = Math.sqrt(rss / a.n);

        outValue.set(String.format("%.4f\t%.4f\t%.4f\t%.4f\t%.2f",
                beta[0], beta[1], beta[2], beta[3], rmse));
        context.write(key, outValue);
    }
}
