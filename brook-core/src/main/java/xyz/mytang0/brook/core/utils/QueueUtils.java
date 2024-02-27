package xyz.mytang0.brook.core.utils;

import xyz.mytang0.brook.common.constants.Delimiter;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

public abstract class QueueUtils {

    private static final String DOMAIN = "queue.domain";

    private static final String TASK_DELAY = "_task_delay_";

    private static final String DECIDE = "_decide_";

    private static final String domain =
            Optional.ofNullable(System.getenv(DOMAIN))
                    .orElseGet(() -> System.getProperty(DOMAIN));

    private QueueUtils() {
    }

    public static String getTaskDelayQueueName() {
        if (StringUtils.isBlank(domain)) {
            return TASK_DELAY;
        }
        return TASK_DELAY + Delimiter.AT + domain;
    }

    public static String getDecideQueueName() {
        if (StringUtils.isBlank(domain)) {
            return DECIDE;
        }
        return DECIDE + Delimiter.AT + domain;
    }

    public static String getDomain() {
        return domain;
    }
}
