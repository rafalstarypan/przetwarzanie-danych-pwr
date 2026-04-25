package pl.pwr.bigdata.mr.stage2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.LinkedHashSet;
import java.util.Set;

public class EventsAggReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        long count = 0;
        Set<String> types = new LinkedHashSet<>();
        for (Text v : values) {
            count += 1;
            String t = v.toString().trim();
            if (!t.isEmpty()) types.add(t);
        }

        String[] kp = key.toString().split("\\|", 2);
        String date = kp[0];
        String borough = kp.length > 1 ? kp[1] : "";

        StringBuilder typesJoined = new StringBuilder();
        boolean first = true;
        for (String t : types) {
            if (!first) typesJoined.append(';');
            typesJoined.append(t);
            first = false;
        }

        outValue.set(date + "\t" + borough + "\t" + count + "\t" + typesJoined);
        context.write(null, outValue);
    }
}
