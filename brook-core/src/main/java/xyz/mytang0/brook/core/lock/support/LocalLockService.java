package xyz.mytang0.brook.core.lock.support;


import xyz.mytang0.brook.common.utils.TimeUtils;
import xyz.mytang0.brook.core.utils.ThreadUtils;
import xyz.mytang0.brook.spi.lock.LockService;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class LocalLockService implements LockService {

    private static final long DEFAULT_EXPIRE_MS = 10 * 1000;

    private static final Map<String, Runnable> releaseTimers =
            new ConcurrentHashMap<>();

    private static final ScheduledThreadPoolExecutor releaseScheduled =
            new ScheduledThreadPoolExecutor(1,
                    ThreadUtils.threadsNamed("local-lock-release-%d"));

    private static final CacheLoader<String, ReentrantLock> LOADER =
            new CacheLoader<String, ReentrantLock>() {
                @Override
                public ReentrantLock load(String key) {
                    assert key.length() != 0;
                    return new ReentrantLock(true);
                }
            };

    private static final LoadingCache<String, ReentrantLock> CACHE =
            CacheBuilder
                    .newBuilder()
                    .build(LOADER);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                releaseScheduled.shutdown();
                releaseTimers.clear();
                CACHE.invalidateAll();
            } catch (Exception ignored) {
            }
        }));
    }

    @Override
    public void acquireLock(String lockId) {
        CACHE.getUnchecked(lockId).lock();
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, TimeUnit unit) {
        return acquireLock(lockId, timeToTry, TimeUnit.MILLISECONDS.convert(DEFAULT_EXPIRE_MS, unit), unit);
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, long leaseTime, TimeUnit unit) {
        boolean acquired = false;
        long startTimeStamp = TimeUtils.currentTimeMillis();
        long leaseTimeStamp = startTimeStamp + unit.toMillis(leaseTime);
        try {
            acquired = CACHE.getUnchecked(lockId).tryLock(timeToTry, unit);
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            if (acquired) {
                if ((startTimeStamp = TimeUtils.currentTimeMillis()) < leaseTimeStamp) {
                    leaseTime = leaseTimeStamp - startTimeStamp;
                    Runnable timer = () -> {
                        try {
                            Optional.ofNullable(releaseTimers.remove(lockId))
                                    .ifPresent(__ ->
                                            CACHE.invalidate(lockId)
                                    );
                        } catch (Exception ignored) {
                        }
                    };
                    // Just overwrite the latest.
                    Runnable oldTimer = releaseTimers.put(lockId, timer);
                    if (oldTimer != null) {
                        releaseScheduled.remove(oldTimer);
                        oldTimer = null;
                    }
                    releaseScheduled.schedule(timer, leaseTime, unit);
                } else {
                    deleteLock(lockId);
                }
            }
        }
    }

    @Override
    public void releaseLock(String lockId) {
        ReentrantLock lock = CACHE.getUnchecked(lockId);
        if (lock != null && lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    @Override
    public void deleteLock(String lockId) {
        try {
            CACHE.invalidate(lockId);
        } finally {
            Optional.ofNullable(releaseTimers.remove(lockId))
                    .ifPresent(releaseScheduled::remove);
        }
    }
}
