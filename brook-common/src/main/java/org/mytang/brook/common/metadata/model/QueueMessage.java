package org.mytang.brook.common.metadata.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class QueueMessage implements Serializable {

    private static final long serialVersionUID = -5779285385830683768L;

    private String type;

    private String id;

    private String payload;

    private int priority;

    private long delayMs;
}
