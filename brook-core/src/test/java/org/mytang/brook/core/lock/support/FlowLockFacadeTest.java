package org.mytang.brook.core.lock.support;

import org.mytang.brook.core.lock.FlowLockFacade;
import org.mytang.brook.core.lock.LockProperties;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FlowLockFacadeTest {
    @Test
    public void testAcquireLock() throws Throwable {
        FlowLockFacade lockFacade =
                new FlowLockFacade(new LockProperties());

        String id = UUID.randomUUID().toString();
        Assert.assertTrue(lockFacade.acquireLock(id));
        Assert.assertTrue(lockFacade.acquireLock(id));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService executorService =
                Executors.newSingleThreadExecutor();

        executorService.execute(() -> {
            int count = 0;
            while (!lockFacade.acquireLock(id)) {
                count++;
                try {
                    Thread.sleep(100);
                } catch (Exception ignored) {
                }
            }
            System.out.println(count);
            countDownLatch.countDown();
        });

        Thread.sleep(4000);
        lockFacade.releaseLock(id);
        lockFacade.releaseLock(id);

        countDownLatch.await();
    }

    @Test
    public void testAcquireLockExpired() throws Throwable {
        LockProperties properties = new LockProperties();
        properties.setLockLeaseTime(Duration.ofMillis(5000));

        FlowLockFacade lockFacade = new FlowLockFacade(properties);

        String id = UUID.randomUUID().toString();
        Assert.assertTrue(lockFacade.acquireLock(id));
        Assert.assertTrue(lockFacade.acquireLock(id));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService executorService =
                Executors.newSingleThreadExecutor();

        executorService.execute(() -> {
            int count = 0;
            while (!lockFacade.acquireLock(id)) {
                count++;
                try {
                    Thread.sleep(100);
                } catch (Exception ignored) {
                }
            }
            System.out.println(count);
            countDownLatch.countDown();
        });

        countDownLatch.await();
    }
}