package xyz.mytang0.brook.core.computing;


import xyz.mytang0.brook.common.exception.BizException;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.common.utils.ExceptionUtils;
import xyz.mytang0.brook.core.exception.TerminateException;
import xyz.mytang0.brook.spi.computing.Engine;
import xyz.mytang0.brook.spi.computing.EngineActuator;
import xyz.mytang0.brook.spi.oss.OSSService;
import xyz.mytang0.brook.spi.oss.OSSStorage;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DefaultEngineActuator implements EngineActuator {

    private final ExtensionLoader<Engine> engineExtensionLoader;

    private final ExtensionLoader<OSSService> ossServiceExtensionLoader;

    public DefaultEngineActuator() {
        this.engineExtensionLoader = ExtensionDirector.getExtensionLoader(Engine.class);
        this.ossServiceExtensionLoader = ExtensionDirector.getExtensionLoader(OSSService.class);
    }

    @Override
    public Map<String, String> introduce() {
        Map<String, String> result = new HashMap<>();
        engineExtensionLoader.getExtensionInstances()
                .forEach(engine ->
                        result.put(engine.type(), engine.introduction())
                );
        return result;
    }

    @Override
    public Object compute(String engineType, String expression, Object input) {
        Engine engine = engineType == null
                ? engineExtensionLoader.getDefaultExtension()
                : engineExtensionLoader.getExtension(engineType);
        if (engine == null) {
            throw new TerminateException(String.format("Engine %s does not exist", engineType));
        }

        try {
            return engine.compute(expression, input);
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause instanceof BizException) {
                throw new TerminateException(rootCause);
            }
            String errorMsg = String.format("Error while evaluating expression: %s", expression);
            log.error(errorMsg, rootCause);
            throw new TerminateException(errorMsg);
        }
    }

    @Override
    public Object compute(String engineType, OSSStorage storage, Object input) {
        OSSService ossService = ossServiceExtensionLoader.getDefaultExtension();
        if (ossService == null) {
            throw new TerminateException("No OSS service implementation exists!");
        }

        StringBuilder stringBuilder = new StringBuilder();
        try (BufferedReader bufferedReader =
                     new BufferedReader(
                             new InputStreamReader(ossService.download(storage)))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }
        } catch (IOException ie) {
            throw new TerminateException(
                    "From OSS service download fail!\n"
                            + ExceptionUtils.getMessage(ie));
        }

        return compute(engineType, stringBuilder.toString(), input);
    }
}
