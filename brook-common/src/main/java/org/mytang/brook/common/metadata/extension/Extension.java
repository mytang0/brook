package org.mytang.brook.common.metadata.extension;

import org.mytang.brook.common.configuration.Validatable;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class Extension extends HashMap<String, String> implements Validatable {

    private static final long serialVersionUID = 8850916840204052004L;

    public Extension() {
        super();
    }

    public Extension(Map<String, String> extension) {
        super(extension);
    }

    @Override
    public void validate() {

    }
}
