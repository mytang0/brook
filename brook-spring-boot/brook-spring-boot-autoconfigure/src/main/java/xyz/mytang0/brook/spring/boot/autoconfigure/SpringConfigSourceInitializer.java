package xyz.mytang0.brook.spring.boot.autoconfigure;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.spi.config.ConfigSource;

import javax.annotation.Nullable;
import java.util.Optional;

public class SpringConfigSourceInitializer implements ConfigSource, EnvironmentPostProcessor, Ordered {

    private static final String NAME = "spring";

    private volatile ConfigurableEnvironment environment;

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        this.environment = environment;
        ExtensionDirector.getExtensionLoader(ConfigSource.class)
                .addExtension(NAME, SpringConfigSourceInitializer.class, this);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Nullable
    @Override
    public Object getProperty(String key) {
        return Optional.ofNullable(environment)
                .map(__ -> __.getProperty(key, Object.class))
                .orElse(null);
    }
}
