package org.mytang.brook.spring.boot.mybatis;

import org.mytang.brook.common.metadata.model.QueueMessage;
import org.mytang.brook.common.utils.TimeUtils;
import org.mytang.brook.spi.annotation.FlowSelectedSPI;
import org.mytang.brook.spi.queue.QueueService;
import org.mytang.brook.spring.boot.mybatis.mapper.QueueMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
@FlowSelectedSPI(name = "mysql")
@ConditionalOnProperty(name = "brook.queue.mysql.enabled", havingValue = "true")
public class MysqlQueueService implements QueueService {

    private final QueueMapper queueMapper;

    public MysqlQueueService(QueueMapper queueMapper) {
        this.queueMapper = queueMapper;
    }

    @Override
    public void offer(String queueName, QueueMessage message) {
        org.mytang.brook.spring.boot.mybatis.entity.QueueMessage
                queueMessage = convert(queueName, message);
        if (0 < queueMapper.insert(queueMessage)) {
            queueMapper.update(queueMessage);
        }
    }

    @Override
    public void offer(String queueName, List<QueueMessage> messages) {
        if (CollectionUtils.isNotEmpty(messages)) {
            int inserts = queueMapper.inserts(
                    messages.stream()
                            .map(message -> convert(queueName, message))
                            .collect(Collectors.toList())
            );

            if (0 < inserts) {
                log.info("Enqueue {} messages successfully", inserts);
            }
        }
    }

    @Override
    public List<QueueMessage> poll(String queueName, int count, long timeout, TimeUnit unit) {

        long startTime = TimeUtils.currentTimeMillis();
        long timeoutTime = startTime + unit.toMillis(timeout);

        List<org.mytang.brook.spring.boot.mybatis.entity.QueueMessage> messages
                = queueMapper.poll(queueName, startTime, count);

        long sleepFor = 500;

        // Avoid infinite loop.
        int maxRetryTimes = 20;

        while (CollectionUtils.isEmpty(messages) && 0 < --maxRetryTimes
                && (0 < (timeoutTime -= TimeUtils.currentTimeMillis()))) {

            sleepFor = Math.min(sleepFor, timeoutTime);

            TimeUtils.sleepUninterruptedly(sleepFor, TimeUnit.MILLISECONDS);

            // Retry.
            messages = queueMapper.poll(queueName, TimeUtils.currentTimeMillis(), count);
        }

        if (CollectionUtils.isNotEmpty(messages)) {

            int popped = queueMapper.popped(queueName, messages.stream().map(
                    org.mytang.brook.spring.boot.mybatis.entity.QueueMessage
                            ::getMessageId
            ).collect(Collectors.toList()));

            if (popped != messages.size()) {
                log.warn("There may be competition, please be idempotent.");
            }

            return messages.stream().map(store -> {
                QueueMessage message = new QueueMessage();
                message.setId(store.getMessageId());
                message.setPayload(store.getPayload());
                return message;
            }).collect(Collectors.toList());
        }

        return Collections.emptyList();
    }

    @Override
    public void remove(String queueName, String messageId) {
        queueMapper.delete(queueName, messageId);
    }

    @Override
    public void remove(String queueName, List<String> messageIds) {
        if (CollectionUtils.isNotEmpty(messageIds)) {
            queueMapper.deletes(queueName, messageIds);
        }
    }

    @Override
    public void unacked(String queueName, long timePoint) {
        queueMapper.unacked(queueName, timePoint);
    }

    private org.mytang.brook.spring.boot.mybatis.entity.QueueMessage convert(
            String queueName,
            QueueMessage message) {
        org.mytang.brook.spring.boot.mybatis.entity.QueueMessage queueMessage =
                new org.mytang.brook.spring.boot.mybatis.entity.QueueMessage();
        queueMessage.setQueueName(queueName);
        queueMessage.setMessageId(message.getId());
        queueMessage.setPriority(message.getPriority());
        if (TimeUtils.currentTimeMillis() < message.getDelayMs()) {
            queueMessage.setDeliveryTime(message.getDelayMs());
        } else {
            queueMessage.setDeliveryTime(
                    TimeUtils.currentTimeMillis() + message.getDelayMs());
        }
        queueMessage.setPayload(message.getPayload());
        return queueMessage;
    }
}
