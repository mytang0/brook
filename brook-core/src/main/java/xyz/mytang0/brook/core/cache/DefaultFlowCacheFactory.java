package xyz.mytang0.brook.core.cache;

import xyz.mytang0.brook.spi.cache.FlowCache;
import xyz.mytang0.brook.spi.cache.FlowCacheFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultFlowCacheFactory implements FlowCacheFactory {

    private final Map<String, FlowCache> cacheMap = new ConcurrentHashMap<>();

    @Override
    public FlowCache getCache(String name) {
        FlowCache cache = cacheMap.get(name);
        if (cache == null) {
            synchronized (this) {
                cache = cacheMap.get(name);
                if (cache == null) {
                    cacheMap.putIfAbsent(name,
                            cache = new DefaultFlowCache());
                }
            }
        }
        return cache;
    }
}
