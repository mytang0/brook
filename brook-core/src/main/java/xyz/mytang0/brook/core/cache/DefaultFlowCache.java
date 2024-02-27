package xyz.mytang0.brook.core.cache;

import xyz.mytang0.brook.spi.cache.FlowCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import xyz.mytang0.brook.core.constants.FlowConstants;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class DefaultFlowCache implements FlowCache {

    private final Cache<Object, Object> cache;

    public DefaultFlowCache() {
        long maximumSize = Optional.ofNullable(
                        Optional.ofNullable(System.getenv(FlowConstants.CACHE_MAX_SIZE_KEY))
                                .orElseGet(() -> System.getProperty(FlowConstants.CACHE_MAX_SIZE_KEY)))
                .map(Long::valueOf)
                .orElse(FlowConstants.CACHE_MAX_SIZE);


        long duration = Optional.ofNullable(
                        Optional.ofNullable(System.getenv(FlowConstants.CACHE_DURATION_KEY))
                                .orElseGet(() -> System.getProperty(FlowConstants.CACHE_DURATION_KEY)))
                .map(Long::valueOf)
                .orElse(FlowConstants.CACHE_DURATION);

        this.cache = Caffeine
                .newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(duration, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public void put(Object key, Object value) {
        cache.put(key, value);
    }

    @Override
    public Object get(Object key) {
        return cache.getIfPresent(key);
    }
}
