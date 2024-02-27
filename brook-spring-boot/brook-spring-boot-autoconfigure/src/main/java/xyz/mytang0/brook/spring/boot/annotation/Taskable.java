package xyz.mytang0.brook.spring.boot.annotation;

import javax.validation.constraints.NotBlank;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;

@Documented
@Target(METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Taskable {

    @NotBlank String type();

    @NotBlank String description();

    boolean isAsync() default false;

    boolean isRetryable() default false;
}
