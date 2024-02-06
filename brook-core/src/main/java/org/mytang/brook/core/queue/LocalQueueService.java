package org.mytang.brook.core.queue;

import org.mytang.brook.common.extension.Disposable;
import org.mytang.brook.common.metadata.model.QueueMessage;
import org.mytang.brook.core.utils.ThreadUtils;
import org.mytang.brook.spi.queue.QueueService;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LocalQueueService implements QueueService, Disposable {

    private final Map<String, ArrayBlockingQueue<QueueMessage>> queueMap
            = new ConcurrentHashMap<>();

    private final Map<String, QueueMessage> delayedMessages
            = new ConcurrentHashMap<>();

    private final ScheduledExecutorService delayedDeliver =
            new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                    ThreadUtils.threadsNamed("local-queue-delayed-deliver-%d"));


    @Override
    public void offer(String queueName, QueueMessage message) {
        if (0 < message.getDelayMs()) {
            delayedDeliver(queueName, message, message.getDelayMs());
        } else {
            getQueue(queueName).add(message);
        }
    }

    private void delayedDeliver(String queueName, QueueMessage message, long delayMs) {
        if (Objects.isNull(delayedMessages.put(message.getId(), message))) {
            try {
                delayedDeliver.schedule(() ->
                                deliver(queueName, message.getId()),
                        delayMs, TimeUnit.MILLISECONDS);
            } catch (Throwable throwable) {
                delayedMessages.remove(message.getId());
                throw throwable;
            }
        }
    }

    private void deliver(String queueName, String messageId) {
        Optional.ofNullable(delayedMessages.remove(messageId))
                .ifPresent(message -> getQueue(queueName).add(message));
    }

    @Override
    public void offer(String queueName, List<QueueMessage> messages) {
        if (CollectionUtils.isNotEmpty(messages)) {
            messages.forEach(message -> offer(queueName, message));
        }
    }

    @Override
    public List<QueueMessage> poll(String queueName, int count, long timeout, TimeUnit unit) {
        if (count < 1) {
            count = 1;
        }
        ArrayList<QueueMessage> polled = new ArrayList<>(count);
        BlockingQueue<QueueMessage> queue = getQueue(queueName);
        try {
            polled.add(queue.poll(timeout, unit));
            count--;
            if (!queue.isEmpty() && 0 < count) {
                queue.drainTo(polled, count);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return polled;
    }

    @Override
    public void remove(String queueName, String messageId) {
        delayedMessages.remove(messageId);
    }

    @Override
    public void remove(String queueName, List<String> messageIds) {
        if (CollectionUtils.isNotEmpty(messageIds)) {
            messageIds.forEach(messageId -> remove(queueName, messageId));
        }
    }

    private ArrayBlockingQueue<QueueMessage> getQueue(String queueName) {
        ArrayBlockingQueue<QueueMessage> queue = queueMap.get(queueName);
        if (queue == null) {
            synchronized (this) {
                queue = queueMap.get(queueName);
                if (queue == null) {
                    queue = queueMap.computeIfAbsent(queueName, __ ->
                            new ArrayBlockingQueue<>(64));
                }
            }
        }
        return queue;
    }

    @Override
    public void destroy() {
        delayedDeliver.shutdown();
        delayedMessages.clear();
        queueMap.values().forEach(
                ArrayBlockingQueue::clear
        );
        queueMap.clear();
    }
}
