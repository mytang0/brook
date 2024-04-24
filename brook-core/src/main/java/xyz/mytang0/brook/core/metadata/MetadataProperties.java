package xyz.mytang0.brook.core.metadata;

import lombok.Data;
import xyz.mytang0.brook.spi.config.ConfigProperties;

@ConfigProperties(prefix = "brook.metadata")
@Data
public class MetadataProperties {

    /**
     * The protocol of the metadata service.
     */
    private String protocol;
}
