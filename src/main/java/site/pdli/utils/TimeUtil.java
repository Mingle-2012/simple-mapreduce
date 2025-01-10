package site.pdli.utils;

public class TimeUtil {

    private boolean started = false;
    private long startTime;

    public void start() {
        if (started) {
            throw new IllegalStateException("Timer already started");
        }
        started = true;
        startTime = System.currentTimeMillis();
    }

    public double stop() {
        if (!started) {
            throw new IllegalStateException("Timer not started");
        }
        started = false;
        return (double) (System.currentTimeMillis() - startTime) / 1000;
    }

    private TimeUtil() {
    }

    private static TimeUtil instance;

    public static TimeUtil getInstance() {
        if (instance == null) {
            instance = new TimeUtil();
        }
        return instance;
    }
}
