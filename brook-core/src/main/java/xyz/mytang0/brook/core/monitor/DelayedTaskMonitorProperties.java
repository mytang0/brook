package xyz.mytang0.brook.core.monitor;

import lombok.Data;

import java.util.Set;

@Data
public class DelayedTaskMonitorProperties {

    /**
     * Queue collection that requires monitor.
     */
    private Set<String> queueProtocols;

    /**
     * The maximum size of messages per pull, the default is 'availableProcessors * 2 + 1'.
     */
    private int pollPerMaxSize =
            Runtime.getRuntime().availableProcessors() * 2 + 1;

    /**
     * Poll timeout, ths default is '2000ms'.
     */
    private int pollTimeoutMs = 2000;

    /**
     * Retry interval in case of exception, ths default is '1000ms'.
     */
    private long exceptionRetryIntervalMs = 1000;

    /**
     * Unacked interval, ths default is '5000ms'.
     */
    private long unackedIntervalMs = 5000;

    /**
     * Unacked timeout, ths default is '60000ms'.
     */
    private long unackedTimeoutMs = 60000;
}
