package org.mytang.brook.common.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class TimeUtils {

    private static final String DATE_TIME_PATTERN = "yyyyMMddHHmmssSSS";

    private static volatile long currentTimeMillis;

    private static volatile String currentDateTime;

    private static volatile boolean isTickerAlive = true;

    public static void sleepUninterruptedly(long sleepFor, TimeUnit unit) {
        boolean interrupted = false;
        try {
            long remainingNanos = unit.toNanos(sleepFor);
            long end = System.nanoTime() + remainingNanos;
            while (true) {
                try {
                    // TimeUnit.sleep() treats negative timeouts just like zero.
                    NANOSECONDS.sleep(remainingNanos);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void sleep(long sleepFor, TimeUnit unit) {
        long remainingNanos = unit.toNanos(sleepFor);
        try {
            NANOSECONDS.sleep(remainingNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static long currentTimeMillis() {
        return currentTimeMillis;
    }

    public static String currentDate() {
        return currentDateTime().substring(0, 8);
    }

    public static String currentDateTime() {
        if (currentDateTime == null) {
            currentDateTime = FastDateFormat.getInstance(DATE_TIME_PATTERN)
                    .format(System.currentTimeMillis());
        }
        return currentDateTime;
    }

    static {
        final FastDateFormat dateTimeFormat =
                FastDateFormat.getInstance(DATE_TIME_PATTERN);
        currentTimeMillis = System.currentTimeMillis();
        currentDateTime = dateTimeFormat.format(currentTimeMillis);
        Thread ticker = new Thread(() -> {
            while (isTickerAlive) {
                currentTimeMillis = System.currentTimeMillis();
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException e) {
                    isTickerAlive = false;
                    Thread.currentThread().interrupt();
                } catch (Exception ignored) {
                    //
                }
            }
        });
        ticker.setDaemon(true);
        ticker.setName("time-millis-ticker-thread");
        ticker.start();
    }
}
