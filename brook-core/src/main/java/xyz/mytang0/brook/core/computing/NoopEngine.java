package xyz.mytang0.brook.core.computing;

import xyz.mytang0.brook.spi.computing.Engine;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopEngine implements Engine {

    static final String TYPE = "noop";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Object compute(String source, Object input) {
        return source;
    }
}
