package xyz.mytang0.brook.spi.config;


import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ConfigProperties {

    String prefix();

    Class<? extends ConfigValidator<?>> validator()
            default ConfigValidator.NULL.class;
}
