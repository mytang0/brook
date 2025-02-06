package xyz.mytang0.brook.spi.limiter;

import xyz.mytang0.brook.common.extension.SPI;

import java.util.concurrent.TimeUnit;

@SPI("default")
public interface RateLimiter {

    /**
     * Set the rate of the current limiter (such as requests per second).
     *
     * @param rate new rate.
     */
    void setRate(double rate);

    /**
     * Get the current rate of the current limiter.
     *
     * @return Current rate.
     */
    double getRate();

    /**
     * Block until a permit is obtained.
     *
     * @return Blocking time (unit: milliseconds).
     */
    default long acquire() {
        return acquire(1);
    }

    /**
     * Block until the specified number of permits are obtained.
     *
     * @param permits Number of permits.
     * @return Blocking time (unit: milliseconds).
     */
    long acquire(int permits);

    /**
     * Get a permit without blocking.
     *
     * @return true: success; false: failure
     */
    @SuppressWarnings("all")
    default boolean tryAcquire() {
        return tryAcquire(1, 0L, TimeUnit.MICROSECONDS);
    }

    /**
     * Wait at most [timeout] to obtain 1 permit.
     *
     * @param timeout The time to wait to get the permits, negative numbers are treated as 0.
     * @param unit    Time unit.
     * @return true: success; false: failure
     */
    default boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    /**
     * Wait at most [timeout] to obtain the specified number of permits.
     *
     * @param permits Number of permits to get.
     * @param timeout The time to wait to get the token, negative numbers are treated as 0.
     * @param unit    Time unit.
     * @return true: success; false: failure
     */
    boolean tryAcquire(int permits, long timeout, TimeUnit unit);
}
