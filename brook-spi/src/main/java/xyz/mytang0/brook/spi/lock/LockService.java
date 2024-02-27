package xyz.mytang0.brook.spi.lock;

import xyz.mytang0.brook.common.extension.SPI;

import java.util.concurrent.TimeUnit;

@SPI("local")
public interface LockService {

    /**
     * Acquires a re-entrant lock on lockId, blocks indefinitely on lockId until it succeeds
     *
     * @param lockId resource to lock on
     */
    void acquireLock(String lockId);

    /**
     * Acquires a re-entrant lock on lockId, blocks for timeToTry duration before giving up
     *
     * @param lockId    resource to lock on
     * @param timeToTry blocks up to timeToTry duration in attempt to acquire the lock
     * @param unit      time unit
     * @return true or false
     */
    boolean acquireLock(String lockId, long timeToTry, TimeUnit unit);

    /**
     * Acquires a re-entrant lock on lockId with provided leaseTime duration. Blocks for timeToTry
     * duration before giving up
     *
     * @param lockId    resource to lock on
     * @param timeToTry blocks up to timeToTry duration in attempt to acquire the lock
     * @param leaseTime LockService lease expiration duration.
     * @param unit      time unit
     * @return true or false
     */
    boolean acquireLock(String lockId, long timeToTry, long leaseTime, TimeUnit unit);

    /**
     * Release a previously acquired lock
     *
     * @param lockId resource to lock on
     */
    void releaseLock(String lockId);

    /**
     * Explicitly cleanup lock resources, if releasing it wouldn't do so.
     *
     * @param lockId resource to lock on
     */
    void deleteLock(String lockId);
}
