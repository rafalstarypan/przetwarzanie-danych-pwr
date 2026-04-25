package pl.pwr.bigdata.mr.stage4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class EnrichMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    static class T3aRow {
        double mean, stddev, q20, q40, q60, q80;
    }

    static class T3bRow {
        double beta0, betaTemp, betaPrecip, betaSnow, rmse;
    }

    static class T3cRow {
        long p33, p66;
    }

    private final Map<String, T3aRow> t3a = new HashMap<>();
    private final Map<String, T3bRow> t3b = new HashMap<>();
    private final Map<String, T3cRow> t3c = new HashMap<>();
    private final Text outValue = new Text();

    @Override
    protected void setup(Context context) throws java.io.IOException {
        File cwd = new File(".");
        File[] all = cwd.listFiles();
        if (all == null) return;
        for (File f : all) {
            String n = f.getName();
            if (n.startsWith("t3a_")) loadT3a(f);
            else if (n.startsWith("t3b_")) loadT3b(f);
            else if (n.startsWith("t3c_")) loadT3c(f);
        }
    }

    private void loadT3a(File path) throws java.io.IOException {
        if (path.isDirectory()) {
            File[] cs = path.listFiles(); if (cs != null) for (File c : cs) loadT3a(c); return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                if (f.length < 7) continue;
                T3aRow r = new T3aRow();
                r.mean = Double.parseDouble(f[1]);
                r.stddev = Double.parseDouble(f[2]);
                r.q20 = Double.parseDouble(f[3]);
                r.q40 = Double.parseDouble(f[4]);
                r.q60 = Double.parseDouble(f[5]);
                r.q80 = Double.parseDouble(f[6]);
                t3a.put(f[0], r);
            }
        }
    }

    private void loadT3b(File path) throws java.io.IOException {
        if (path.isDirectory()) {
            File[] cs = path.listFiles(); if (cs != null) for (File c : cs) loadT3b(c); return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                if (f.length < 6) continue;
                T3bRow r = new T3bRow();
                r.beta0 = Double.parseDouble(f[1]);
                r.betaTemp = Double.parseDouble(f[2]);
                r.betaPrecip = Double.parseDouble(f[3]);
                r.betaSnow = Double.parseDouble(f[4]);
                r.rmse = Double.parseDouble(f[5]);
                t3b.put(f[0], r);
            }
        }
    }

    private void loadT3c(File path) throws java.io.IOException {
        if (path.isDirectory()) {
            File[] cs = path.listFiles(); if (cs != null) for (File c : cs) loadT3c(c); return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] f = line.split("\t", -1);
                if (f.length < 3) continue;
                T3cRow r = new T3cRow();
                r.p33 = Long.parseLong(f[1]);
                r.p66 = Long.parseLong(f[2]);
                t3c.put(f[0], r);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;
        String[] f = line.split("\t", -1);
        if (f.length < 10) return;

        String date = f[0];
        String borough = f[1];
        long total;
        try { total = Long.parseLong(f[2]); } catch (NumberFormatException e) { return; }
        String members = f[3];
        String avgDur = f[4];
        String tempStr = f[5];
        String precipStr = f[6];
        String snowStr = f[7];
        long evCount;
        try { evCount = Long.parseLong(f[8]); } catch (NumberFormatException e) { evCount = 0; }
        String evTypes = f[9];

        T3aRow stats = t3a.get(borough);
        T3bRow reg = t3b.get(borough);
        T3cRow ev = t3c.get(borough);

        String expectedStr = "";
        if (reg != null && !tempStr.isEmpty()) {
            double temp = Double.parseDouble(tempStr);
            double precip = precipStr.isEmpty() ? 0.0 : Double.parseDouble(precipStr);
            double snow = snowStr.isEmpty() ? 0.0 : Double.parseDouble(snowStr);
            double expected = reg.beta0 + reg.betaTemp * temp + reg.betaPrecip * precip + reg.betaSnow * snow;
            if (expected < 0) expected = 0;
            expectedStr = String.format("%.0f", expected);
        }

        String intensity;
        if (evCount == 0) intensity = "none";
        else if (ev == null) intensity = "low";
        else if (evCount <= ev.p33) intensity = "low";
        else if (evCount <= ev.p66) intensity = "medium";
        else intensity = "high";

        String anomaly = "false";
        String demand = "average";
        if (stats != null) {
            if (Math.abs(total - stats.mean) > 2 * stats.stddev) anomaly = "true";
            if (total <= stats.q20) demand = "low";
            else if (total <= stats.q40) demand = "below_average";
            else if (total <= stats.q60) demand = "average";
            else if (total <= stats.q80) demand = "above_average";
            else demand = "high";
        }

        outValue.set(date + "\t" + borough + "\t" + total + "\t" + members + "\t" + avgDur
                + "\t" + tempStr + "\t" + precipStr + "\t" + snowStr
                + "\t" + evTypes + "\t" + expectedStr
                + "\t" + intensity + "\t" + anomaly + "\t" + demand);
        context.write(NullWritable.get(), outValue);
    }
}
