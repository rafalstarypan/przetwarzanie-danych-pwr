package pl.pwr.bigdata.mr.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NoaaJsonReader {

    public static final class DailyWeather {
        public Double tmaxC;
        public Double tminC;
        public Double prcpMm;
        public Double snowMm;

        public Double tempAvgC() {
            if (tmaxC != null && tminC != null) return (tmaxC + tminC) / 2.0;
            if (tmaxC != null) return tmaxC;
            if (tminC != null) return tminC;
            return null;
        }
    }

    private static final Pattern OBJECT = Pattern.compile("\\{[^{}]*\\}", Pattern.DOTALL);
    private static final Pattern DATE = Pattern.compile("\"date\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern DATATYPE = Pattern.compile("\"datatype\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern VALUE = Pattern.compile("\"value\"\\s*:\\s*(-?\\d+(?:\\.\\d+)?)");

    private NoaaJsonReader() {}

    public static Map<String, DailyWeather> readFile(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        return parse(new String(bytes, StandardCharsets.UTF_8));
    }

    public static Map<String, DailyWeather> readStream(InputStream in) throws IOException {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int n;
        while ((n = in.read(chunk)) > 0) buf.write(chunk, 0, n);
        return parse(new String(buf.toByteArray(), StandardCharsets.UTF_8));
    }

    public static Map<String, DailyWeather> parse(String json) {
        Map<String, DailyWeather> out = new HashMap<>();
        Matcher mObj = OBJECT.matcher(json);
        while (mObj.find()) {
            String obj = mObj.group();
            Matcher mDate = DATE.matcher(obj);
            Matcher mType = DATATYPE.matcher(obj);
            Matcher mVal = VALUE.matcher(obj);
            if (!mDate.find() || !mType.find() || !mVal.find()) continue;
            String date = mDate.group(1);
            if (date.length() >= 10) date = date.substring(0, 10);
            String type = mType.group(1);
            double value = Double.parseDouble(mVal.group(1));
            DailyWeather d = out.computeIfAbsent(date, k -> new DailyWeather());
            switch (type) {
                case "TMAX": d.tmaxC = value / 10.0; break;
                case "TMIN": d.tminC = value / 10.0; break;
                case "PRCP": d.prcpMm = value / 10.0; break;
                case "SNOW": d.snowMm = value; break;
                default: break;
            }
        }
        return out;
    }
}
