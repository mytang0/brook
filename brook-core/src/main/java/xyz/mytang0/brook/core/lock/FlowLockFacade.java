package xyz.mytang0.brook.core.lock;

import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.spi.lock.LockService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class FlowLockFacade {

    private final LockProperties properties;

    private final long lockLeaseTime;

    private final long lockTimeToTry;

    private volatile LockService lockService;

    public FlowLockFacade(LockProperties properties) {
        this.properties = properties;
        this.lockLeaseTime = properties.getLockLeaseTime().toMillis();
        this.lockTimeToTry = properties.getLockTimeToTry().toMillis();
    }

    public boolean acquireLock(String lockId) {
        return acquireLock(lockId, lockTimeToTry, lockLeaseTime);
    }

    public boolean acquireLock(String lockId, long timeToTryMs) {
        return acquireLock(lockId, timeToTryMs, lockLeaseTime);
    }

    public boolean acquireLock(String lockId, long timeToTryMs, long leaseTimeMs) {
        if (properties.isEnabled()) {
            loadLockService();
            return lockService.acquireLock(
                    lockId, timeToTryMs, leaseTimeMs, TimeUnit.MILLISECONDS);
        }
        return true;
    }

    public void releaseLock(String lockId) {
        if (properties.isEnabled()) {
            loadLockService();
            lockService.releaseLock(lockId);
        }
    }

    public void deleteLock(String lockId) {
        if (properties.isEnabled()) {
            loadLockService();
            lockService.deleteLock(lockId);
        }
    }

    private void loadLockService() {
        if (lockService == null) {
            synchronized (this) {
                if (lockService == null) {
                    this.lockService = ExtensionLoader.getExtension(
                            LockService.class,
                            properties.getProtocol()
                    );
                }
            }
        }
    }
}
