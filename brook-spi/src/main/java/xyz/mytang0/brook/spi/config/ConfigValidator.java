package xyz.mytang0.brook.spi.config;

import javax.annotation.Nullable;

public interface ConfigValidator<T> {

    void validate(@Nullable T target);


    class NULL implements ConfigValidator<Object> {

        @Override
        public void validate(@Nullable Object target) {

        }
    }
}
