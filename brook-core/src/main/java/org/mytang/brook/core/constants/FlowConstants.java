package org.mytang.brook.core.constants;

public interface FlowConstants {

    String FLOW = "flow";

    String ID = "id";

    String DEF = "def";

    String STATUS = "status";

    String INPUT = "input";

    String OUTPUT = "output";

    String CREATOR = "creator";

    String INSTANCE = "instance";

    String EXTENSION = "extension";

    // Engine
    String DEFAULT_ENGINE_TYPE = "javascript";

    // Cache
    String CACHE_MAX_SIZE_KEY = "flow.cache.max-size";

    // Queue Default Protocol
    String QUEUE_DEFAULT = "local";

    long CACHE_MAX_SIZE = 1000L;

    String CACHE_DURATION_KEY = "flow.cache.duration";

    long CACHE_DURATION = 300L;

    // Request constants
    long DEFAULT_TIMEOUT_MS = 5000;

    long LOCK_TRY_TIME_MS = 30000;
}
