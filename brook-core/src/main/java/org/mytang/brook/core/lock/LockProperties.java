package org.mytang.brook.core.lock;

import lombok.Data;

import java.time.Duration;

@Data
public class LockProperties {

    /**
     * Whether to enable flow-lock, the default is 'true'.
     */
    private boolean enabled = true;

    /**
     * The protocol of the flow-lock. Specify flow-lock implementation name. If empty, the default implementation is used.
     */
    private String protocol;

    /**
     * The maximum lock occupancy time. When the time is reached, the lock will be automatically released. The default is 60000 millis.
     */
    private Duration lockLeaseTime = Duration.ofMillis(60000);

    /**
     * Maximum lock acquisition time. After the time is reached, the wait ends even if the lock is not acquired. The default value is 500 milliseconds.
     */
    private Duration lockTimeToTry = Duration.ofMillis(500);
}
