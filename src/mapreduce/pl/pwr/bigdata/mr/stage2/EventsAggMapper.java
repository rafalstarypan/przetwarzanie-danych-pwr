package pl.pwr.bigdata.mr.stage2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pl.pwr.bigdata.mr.common.CsvParser;
import pl.pwr.bigdata.mr.common.DateUtils;

public class EventsAggMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int COL_START_DATE_TIME = 2;
    private static final int COL_EVENT_TYPE = 5;
    private static final int COL_EVENT_BOROUGH = 6;

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        if (line.isEmpty()) return;
        if (line.startsWith("event_id")) return;
        if (line.startsWith("#")) return;

        String[] f = CsvParser.parse(line);
        if (f.length <= COL_EVENT_BOROUGH) {
            context.getCounter("E2b", "dropped_short_row").increment(1);
            return;
        }

        String start = f[COL_START_DATE_TIME].trim();
        String borough = f[COL_EVENT_BOROUGH].trim();
        String type = f[COL_EVENT_TYPE].trim();
        if (start.isEmpty() || borough.isEmpty()) {
            context.getCounter("E2b", "dropped_empty").increment(1);
            return;
        }

        String date;
        try {
            date = DateUtils.isoDateFromIsoTimestamp(start);
        } catch (java.text.ParseException e) {
            context.getCounter("E2b", "dropped_bad_timestamp").increment(1);
            return;
        }

        outKey.set(date + "|" + borough);
        outValue.set(type);
        context.write(outKey, outValue);
    }
}
