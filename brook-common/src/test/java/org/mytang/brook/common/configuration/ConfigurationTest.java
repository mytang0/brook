package org.mytang.brook.common.configuration;

import org.mytang.brook.common.metadata.extension.Extension;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ConfigurationTest {

    private static final ConfigOption<Extension> EXTENSION =
            ConfigOptions.key("extension")
                    .classType(Extension.class)
                    .noDefaultValue();

    @Test
    public void getOptional() {
        Extension extension = new Extension();
        extension.put("name", "brook");

        Map<String, Object> properties = new HashMap<>();
        properties.put(EXTENSION.key(), extension);
        Configuration configuration = new Configuration(properties);

        Optional<Extension> extensionOptional =
                configuration.getOptional(EXTENSION);

        Assert.assertTrue(extensionOptional.isPresent());
    }
}
