package pl.pwr.bigdata.mr.stage1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pl.pwr.bigdata.mr.common.CsvParser;
import pl.pwr.bigdata.mr.common.DateUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final int COL_RIDE_ID = 0;
    private static final int COL_RIDEABLE = 1;
    private static final int COL_STARTED_AT = 2;
    private static final int COL_ENDED_AT = 3;
    private static final int COL_START_STATION_ID = 5;
    private static final int COL_MEMBER_CASUAL = 12;

    private final Map<String, String> stationToBorough = new HashMap<>();
    private final Text outValue = new Text();

    @Override
    protected void setup(Context context) throws java.io.IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new java.io.IOException("Stations DistributedCache file not provided");
        }
        String alias = "stations.csv";
        try (BufferedReader br = new BufferedReader(new FileReader(alias))) {
            String line;
            boolean headerSkipped = false;
            while ((line = br.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    if (line.startsWith("start_station_id")) continue;
                }
                String[] f = CsvParser.parse(line);
                if (f.length < 5) continue;
                stationToBorough.put(f[0].trim(), f[4].trim());
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;
        if (line.startsWith("ride_id")) return;

        String[] f = CsvParser.parse(line);
        if (f.length <= COL_MEMBER_CASUAL) {
            context.getCounter("E1", "dropped_short_row").increment(1);
            return;
        }

        String stationId = f[COL_START_STATION_ID].trim();
        if (stationId.isEmpty()) {
            context.getCounter("E1", "dropped_no_station").increment(1);
            return;
        }

        String startedAt = f[COL_STARTED_AT].trim();
        String endedAt = f[COL_ENDED_AT].trim();
        if (startedAt.length() < 10 || endedAt.length() < 10) {
            context.getCounter("E1", "dropped_bad_timestamp").increment(1);
            return;
        }

        long startMs, endMs;
        try {
            startMs = DateUtils.parseCitibikeTimestampMs(startedAt);
            endMs = DateUtils.parseCitibikeTimestampMs(endedAt);
        } catch (java.text.ParseException e) {
            context.getCounter("E1", "dropped_bad_timestamp").increment(1);
            return;
        }

        double durMin = (endMs - startMs) / 60000.0;
        if (durMin < 1.0) {
            context.getCounter("E1", "dropped_too_short").increment(1);
            return;
        }
        if (durMin > 1440.0) {
            context.getCounter("E1", "dropped_too_long").increment(1);
            return;
        }

        String date = DateUtils.extractDate(startedAt);
        String borough = stationToBorough.getOrDefault(stationId, "Other");
        String rideable = f[COL_RIDEABLE].trim();
        String memberCasual = f[COL_MEMBER_CASUAL].trim();
        String rideId = f[COL_RIDE_ID].trim();

        outValue.set(rideId + "\t" + date + "\t" + borough + "\t" + rideable
                + "\t" + memberCasual + "\t" + String.format("%.2f", durMin)
                + "\t" + stationId);
        context.write(NullWritable.get(), outValue);
        context.getCounter("E1", "kept").increment(1);
    }
}
