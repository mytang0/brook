package org.mytang.brook.lock.redis;

import org.mytang.brook.common.extension.Disposable;
import org.mytang.brook.spi.annotation.FlowSPI;
import org.mytang.brook.spi.lock.LockService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

@Slf4j
@FlowSPI(name = "redis")
public class RedisLockService implements LockService, Disposable {

    private volatile RedissonClient redisson;

    public RedisLockService() {
    }

    public RedisLockService(final Config config) {
        setConfig(config);
    }

    public void setConfig(Config config) {
        if (redisson == null) {
            synchronized (this) {
                if (redisson == null) {
                    this.redisson = Redisson.create(config);
                }
            }
        }
    }

    @Override
    public void acquireLock(String lockId) {
        validate();
        redisson.getLock(lockId).lock();
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, TimeUnit unit) {
        validate();
        RLock lock = redisson.getLock(lockId);
        try {
            return lock.tryLock(timeToTry, unit);
        } catch (Exception e) {
            log.error(String.format("Acquire lock: %s exception", lockId), e);
            return false;
        }
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, long leaseTime, TimeUnit unit) {
        validate();
        RLock lock = redisson.getLock(lockId);
        try {
            return lock.tryLock(timeToTry, leaseTime, unit);
        } catch (Exception e) {
            log.error(String.format("Acquire lock: %s exception", lockId), e);
            return false;
        }
    }

    @Override
    public void releaseLock(String lockId) {
        validate();
        // Multiple releases can cause exceptions.
        try {
            redisson.getLock(lockId).unlock();
        } catch (Exception ignored) {
        }
    }

    @Override
    public void deleteLock(String lockId) {
        //
    }

    boolean isHeldByCurrentThread(String lockId) {
        validate();
        return redisson.getLock(lockId)
                .isHeldByCurrentThread();
    }

    @Override
    public void destroy() {
        if (redisson != null) {
            redisson.shutdown();
        }
    }

    private void validate() {
        if (redisson == null) {
            throw new IllegalStateException("Uninitialized");
        }
    }
}
