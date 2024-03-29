package xyz.mytang0.brook.spi.computing;

import xyz.mytang0.brook.common.extension.SPI;
import xyz.mytang0.brook.spi.oss.OSSStorage;

import java.util.Map;

@SPI(value = "default")
public interface EngineActuator {

    Map<String, String> introduce();

    Object compute(String engineType, String expression, Object input);

    default Object compute(String engineType, OSSStorage storage, Object input) {
        throw new IllegalStateException(
                "The engine actuator does not support execution via storage!");
    }
}
