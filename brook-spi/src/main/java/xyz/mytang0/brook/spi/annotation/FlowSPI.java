package xyz.mytang0.brook.spi.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

@Documented
@Target(TYPE)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface FlowSPI {
    String name();
}
