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

        double varX1 = a.sx1sq - (a.sx1 * a.sx1) / a.n;
        double varX2 = a.sx2sq - (a.sx2 * a.sx2) / a.n;
        double varX3 = a.sx3sq - (a.sx3 * a.sx3) / a.n;
        boolean useTemp = varX1 > 1e-9;
        boolean usePrecip = varX2 > 1e-9;
        boolean useSnow = varX3 > 1e-9;

        int dim = 1 + (useTemp ? 1 : 0) + (usePrecip ? 1 : 0) + (useSnow ? 1 : 0);
        int colTemp = -1, colPrecip = -1, colSnow = -1;
        int next = 1;
        if (useTemp) colTemp = next++;
        if (usePrecip) colPrecip = next++;
        if (useSnow) colSnow = next++;

        double[][] xtx = new double[dim][dim];
        double[] xty = new double[dim];
        xtx[0][0] = a.n;
        xty[0] = a.sy;
        if (useTemp) {
            xtx[0][colTemp] = a.sx1; xtx[colTemp][0] = a.sx1;
            xtx[colTemp][colTemp] = a.sx1sq; xty[colTemp] = a.sx1y;
        }
        if (usePrecip) {
            xtx[0][colPrecip] = a.sx2; xtx[colPrecip][0] = a.sx2;
            xtx[colPrecip][colPrecip] = a.sx2sq; xty[colPrecip] = a.sx2y;
            if (useTemp) { xtx[colTemp][colPrecip] = a.sx1x2; xtx[colPrecip][colTemp] = a.sx1x2; }
        }
        if (useSnow) {
            xtx[0][colSnow] = a.sx3; xtx[colSnow][0] = a.sx3;
            xtx[colSnow][colSnow] = a.sx3sq; xty[colSnow] = a.sx3y;
            if (useTemp) { xtx[colTemp][colSnow] = a.sx1x3; xtx[colSnow][colTemp] = a.sx1x3; }
            if (usePrecip) { xtx[colPrecip][colSnow] = a.sx2x3; xtx[colSnow][colPrecip] = a.sx2x3; }
        }

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
        if (!useTemp) context.getCounter("E3b", "dropped_temp_zero_variance").increment(1);
        if (!usePrecip) context.getCounter("E3b", "dropped_precip_zero_variance").increment(1);
        if (!useSnow) context.getCounter("E3b", "dropped_snow_zero_variance").increment(1);

        double b0 = beta[0];
        double b1 = useTemp ? beta[colTemp] : 0.0;
        double b2 = usePrecip ? beta[colPrecip] : 0.0;
        double b3 = useSnow ? beta[colSnow] : 0.0;

        double rss = a.sysq - (b0 * a.sy + b1 * a.sx1y + b2 * a.sx2y + b3 * a.sx3y);
        if (rss < 0) rss = 0;
        double rmse = Math.sqrt(rss / a.n);

        outValue.set(String.format("%.4f\t%.4f\t%.4f\t%.4f\t%.2f", b0, b1, b2, b3, rmse));
        context.write(key, outValue);
    }
}
