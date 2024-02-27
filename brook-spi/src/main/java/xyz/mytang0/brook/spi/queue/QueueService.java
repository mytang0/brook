package xyz.mytang0.brook.spi.queue;

import xyz.mytang0.brook.common.extension.SPI;
import xyz.mytang0.brook.common.metadata.model.QueueMessage;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static xyz.mytang0.brook.common.constants.QueueConstants.DEFAULT_POSTPONE;

@SPI(value = "local")
public interface QueueService {

    void offer(String queueName, QueueMessage message);

    default void offer(String queueName, List<QueueMessage> messages) {
        if (CollectionUtils.isNotEmpty(messages)) {
            messages.forEach(message ->
                    offer(queueName, message));
        }
    }

    List<QueueMessage> poll(String queueName, int count, long timeout, TimeUnit unit);

    void remove(String queueName, String messageId);

    void remove(String queueName, List<String> messageIds);

    default void postpone(String queueName, QueueMessage message) {
        remove(queueName, message.getId());
        if (message.getDelayMs() < 1) {
            // Set default value.
            message.setDelayMs(DEFAULT_POSTPONE);
        }
        offer(queueName, message);
    }

    default boolean ack(String queueName, String messageId) {
        remove(queueName, messageId);
        return true;
    }

    default void unacked(String queueName, long timePoint) {
    }
}
