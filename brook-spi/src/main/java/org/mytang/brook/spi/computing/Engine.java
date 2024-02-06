package org.mytang.brook.spi.computing;

import org.mytang.brook.common.extension.SPI;
import org.mytang.brook.spi.oss.OSSStorage;

import javax.validation.constraints.NotBlank;

@SPI
public interface Engine {

    @NotBlank String type();

    default String introduction() {
        return null;
    }

    default void validate(String source) {
    }

    Object compute(String source, Object input) throws Exception;

    default Object compute(OSSStorage storage, Object input) throws Exception {
        throw new IllegalStateException(
                "The engine does not support execution via storage!");
    }
}