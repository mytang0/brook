package xyz.mytang0.brook.core.limiter;

import xyz.mytang0.brook.spi.limiter.RateLimiter;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("all")
public class DefaultRateLimiter implements RateLimiter {

    private volatile com.google.common.util.concurrent.RateLimiter rateLimiter;

    @Override
    public void setRate(double rate) {
        rateLimiter = com.google.common.util.concurrent.RateLimiter.create(rate);
    }

    @Override
    public double getRate() {
        stateCheck();
        return rateLimiter.getRate();
    }

    @Override
    public long acquire(int permits) {
        stateCheck();
        return (long) (rateLimiter.acquire(permits) * 1000);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        stateCheck();
        return rateLimiter.tryAcquire(permits, timeout, unit);
    }

    private void stateCheck() {
        if (rateLimiter == null) {
            throw new IllegalStateException(
                    "The current limiter has no current limit rate set.");
        }
    }
}
