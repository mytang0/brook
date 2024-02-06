package org.mytang.brook.spring.boot.metadata.http;

import org.mytang.brook.metadata.http.HTTPMetadataConfig;
import org.mytang.brook.metadata.http.HTTPMetadataService;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

public class HTTPMetadataAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "brook.metadata.http")
    public HTTPMetadataConfig metadataConfig() {
        return new HTTPMetadataConfig();
    }

    @Bean
    public HTTPMetadataService metadataService(HTTPMetadataConfig config) {
        return new HTTPMetadataService(config);
    }
}
