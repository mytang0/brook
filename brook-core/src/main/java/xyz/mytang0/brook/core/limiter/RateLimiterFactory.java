package xyz.mytang0.brook.core.limiter;

import xyz.mytang0.brook.common.constants.Delimiter;
import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.spi.limiter.RateLimiter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RateLimiterFactory {

    private final Map<String, RateLimiter> rateLimiters
            = new ConcurrentHashMap<>();

    private final RateLimiterProperties properties;

    public RateLimiterFactory(RateLimiterProperties properties) {
        this.properties = properties;
    }

    public RateLimiter getRateLimiter(final FlowDef flowDef) {
        return getRateLimiter(
                flowDef.getName()
                        + Delimiter.AT
                        + flowDef.getVersion(),
                flowDef.getControlDef().getConcurrencyLimit());
    }

    public RateLimiter getRateLimiter(final TaskDef taskDef) {
        return getRateLimiter(
                taskDef.getName(),
                taskDef.getControlDef().getConcurrencyLimit());
    }

    private RateLimiter getRateLimiter(String key, double rate) {
        RateLimiter limiter = rateLimiters.get(key);
        if (limiter == null) {
            synchronized (this) {
                limiter = rateLimiters.get(key);
                if (limiter == null) {
                    limiter = ExtensionLoader.getExtension(
                            RateLimiter.class,
                            properties.getProtocol());

                    limiter.setRate(rate);

                    rateLimiters.put(key, limiter);
                }
            }
        }
        return limiter;
    }
}
