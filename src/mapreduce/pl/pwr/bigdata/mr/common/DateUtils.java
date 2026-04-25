package pl.pwr.bigdata.mr.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class DateUtils {

    private static final ThreadLocal<SimpleDateFormat> CITIBIKE_TS = ThreadLocal.withInitial(() -> {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        f.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        f.setLenient(false);
        return f;
    });

    private static final ThreadLocal<SimpleDateFormat> CITIBIKE_TS_MS = ThreadLocal.withInitial(() -> {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        f.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        f.setLenient(false);
        return f;
    });

    private static final ThreadLocal<SimpleDateFormat> ISO_TS = ThreadLocal.withInitial(() -> {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        f.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        f.setLenient(false);
        return f;
    });

    private DateUtils() {}

    public static long parseCitibikeTimestampMs(String s) throws java.text.ParseException {
        if (s.length() > 19 && s.charAt(19) == '.') {
            return CITIBIKE_TS_MS.get().parse(s).getTime();
        }
        return CITIBIKE_TS.get().parse(s).getTime();
    }

    public static String extractDate(String timestamp) {
        return timestamp.length() >= 10 ? timestamp.substring(0, 10) : timestamp;
    }

    public static String isoDateFromIsoTimestamp(String s) throws java.text.ParseException {
        String trimmed = s;
        int dot = trimmed.indexOf('.');
        if (dot > 0) trimmed = trimmed.substring(0, dot);
        if (trimmed.endsWith("Z")) trimmed = trimmed.substring(0, trimmed.length() - 1);
        Date d = ISO_TS.get().parse(trimmed);
        SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd");
        out.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        return out.format(d);
    }
}
