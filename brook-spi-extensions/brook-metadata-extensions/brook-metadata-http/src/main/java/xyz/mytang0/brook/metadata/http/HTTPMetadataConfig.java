package xyz.mytang0.brook.metadata.http;

import xyz.mytang0.brook.common.utils.StringUtils;
import lombok.Data;
import xyz.mytang0.brook.spi.config.ConfigProperties;

import java.time.Duration;

@ConfigProperties(prefix = "brook.metadata.http")
@Data
public class HTTPMetadataConfig {

    /**
     * The metadata server uri.
     */
    private String serverUri;

    /**
     * We obtain metadata from the metadata center through a http get request.
     * This attribute identifies the request key of the metadata. The default is 'name'.
     */
    private String nameKey = "name";

    /**
     * We obtain metadata from the metadata center through a http get request.
     * This attribute identifies the request key of the metadata. The default is 'version'.
     */
    private String versionKey = "version";

    /**
     * The response result is wrapped.
     */
    private boolean wrapped = true;

    /**
     * Whether to enable result caching. The default is 'true'.
     */
    private boolean enableCache = true;

    /**
     * Cache expiration duration, the default is '10m'.
     */
    private Duration cacheExpiredDuration = Duration.ofMinutes(10);

    /**
     * Cache maximum size, the default is '100'.
     */
    private long cacheMaximumSize = 100;

    public void validate() {
        if (StringUtils.isBlank(serverUri)) {
            throw new IllegalArgumentException(
                    "The 'serverUri' is blank");
        }

        if (StringUtils.isBlank(nameKey)) {
            throw new IllegalArgumentException(
                    "The 'nameKey' is blank");
        }
    }
}
