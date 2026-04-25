package pl.pwr.bigdata.mr.stage2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pl.pwr.bigdata.mr.common.NoaaJsonReader;
import pl.pwr.bigdata.mr.common.NoaaJsonReader.DailyWeather;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class JoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private final Map<String, DailyWeather> weather = new HashMap<>();
    private final Map<String, String[]> events = new HashMap<>();
    private final Text outValue = new Text();

    @Override
    protected void setup(Context context) throws java.io.IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null) return;

        File cwd = new File(".");
        File[] localFiles = cwd.listFiles();
        if (localFiles == null) return;

        for (File f : localFiles) {
            String name = f.getName();
            if (name.startsWith("noaa_") && (name.endsWith(".json") || f.isDirectory())) {
                loadNoaa(f);
            } else if (name.startsWith("events_") || name.equals("t2_events")) {
                loadEvents(f);
            }
        }
    }

    private void loadNoaa(File path) throws java.io.IOException {
        if (path.isDirectory()) {
            File[] children = path.listFiles();
            if (children != null) for (File c : children) loadNoaa(c);
            return;
        }
        Map<String, DailyWeather> m = NoaaJsonReader.readFile(path.toPath());
        for (Map.Entry<String, DailyWeather> e : m.entrySet()) {
            weather.merge(e.getKey(), e.getValue(), (a, b) -> {
                if (b.tmaxC != null) a.tmaxC = b.tmaxC;
                if (b.tminC != null) a.tminC = b.tminC;
                if (b.prcpMm != null) a.prcpMm = b.prcpMm;
                if (b.snowMm != null) a.snowMm = b.snowMm;
                return a;
            });
        }
    }

    private void loadEvents(File path) throws java.io.IOException {
        if (path.isDirectory()) {
            File[] children = path.listFiles();
            if (children != null) for (File c : children) loadEvents(c);
            return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("_SUCCESS") || line.startsWith("#")) continue;
                String[] f = line.split("\t", -1);
                if (f.length < 4) continue;
                String date = f[0];
                String borough = f[1];
                String count = f[2];
                String types = f[3];
                events.put(date + "|" + borough, new String[]{count, types});
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;

        String[] f = line.split("\t", -1);
        if (f.length < 5) return;

        String date = f[0];
        String borough = f[1];
        String total = f[2];
        String members = f[3];
        String avgDur = f[4];

        DailyWeather w = weather.get(date);
        String tempStr = (w != null && w.tempAvgC() != null) ? String.format("%.2f", w.tempAvgC()) : "";
        String prcpStr = (w != null && w.prcpMm != null) ? String.format("%.2f", w.prcpMm) : "0.00";
        String snowStr = (w != null && w.snowMm != null) ? String.format("%.2f", w.snowMm) : "0.00";

        String[] ev = events.get(date + "|" + borough);
        String evCount = ev != null ? ev[0] : "0";
        String evTypes = ev != null ? ev[1] : "";

        outValue.set(date + "\t" + borough + "\t" + total + "\t" + members + "\t" + avgDur
                + "\t" + tempStr + "\t" + prcpStr + "\t" + snowStr
                + "\t" + evCount + "\t" + evTypes);
        context.write(NullWritable.get(), outValue);
    }
}
