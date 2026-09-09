package xyz.mytang0.brook.common.metadata.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class QueueMessage implements Serializable {

    private static final long serialVersionUID = -5779285385830683768L;

    private String type;

    private String id;

    /**
     * Stable key used by transports to deduplicate a delivery.
     */
    private String deduplicationKey;

    private String payload;

    private int priority;

    private long delayMs;

    /**
     * Absolute availability timestamp, when known by the producer.
     */
    private long availableAt;

    /**
     * The task execution attempt represented by this message.
     */
    private int attempt;
}
