package xyz.mytang0.brook.core.limiter;

import lombok.Data;
import xyz.mytang0.brook.spi.config.ConfigProperties;

@ConfigProperties(prefix = "brook.limiter")
@Data
public class RateLimiterProperties {

    /**
     * The protocol of the rate-limiter. Specify rate-limiter implementation name. If empty, the default implementation is used.
     */
    private String protocol;
}
