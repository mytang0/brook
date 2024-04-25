package xyz.mytang0.brook.task.http;

import lombok.Data;
import xyz.mytang0.brook.spi.config.ConfigProperties;

@ConfigProperties(prefix = "brook.task.http")
@Data
public class HTTPTaskConfig {

    /**
     * Define some configurations related to the HTTP client.
     */
    private ClientConfig clientConfig;

    /**
     * Define some configuration related to the task.
     */
    private TaskConfig taskConfig;

    @Data
    public static class ClientConfig {

        private int connectionRequestTimeout = 6000;

        private int socketTimeout = 30000;

        private int connectTimeout = 10000;

        private int maxConnTotal = 1024;

        private int maxConnPerRoute = 1024;
    }

    @Data
    public static class TaskConfig {

    }
}
