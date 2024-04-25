package xyz.mytang0.brook.metadata.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import xyz.mytang0.brook.common.constants.Delimiter;
import xyz.mytang0.brook.common.extension.Disposable;
import xyz.mytang0.brook.common.extension.Selected;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.spi.config.ConfiguratorFacade;
import xyz.mytang0.brook.spi.metadata.MetadataService;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Selected
public class HTTPMetadataService implements MetadataService, Disposable {

    private final FutureCallback<HttpResponse> EMPTY_CALLBACK =
            new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse result) {
                }

                @Override
                public void failed(Exception ex) {
                }

                @Override
                public void cancelled() {
                }
            };

    private volatile HTTPMetadataConfig config;

    private volatile CloseableHttpAsyncClient httpAsyncClient;

    private volatile Cache<String, FlowDef> cache;

    public HTTPMetadataService() {
        this(ConfiguratorFacade
                .getConfig(HTTPMetadataConfig.class));
    }

    public HTTPMetadataService(final HTTPMetadataConfig config) {
        initialize(config);
    }

    public void initialize(final HTTPMetadataConfig config) {

        Objects.requireNonNull(config);

        config.validate();

        this.config = config;

        this.httpAsyncClient = HttpAsyncClients.custom()
                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy() {
                    @Override
                    public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                        long keepAlive = super.getKeepAliveDuration(response, context);
                        if (keepAlive == -1) {
                            keepAlive = 10 * 1000;
                        }
                        return keepAlive;
                    }
                })
                .setDefaultIOReactorConfig(
                        IOReactorConfig.custom()
                                .setSoReuseAddress(true)
                                .setTcpNoDelay(true)
                                .build()
                ).build();

        if (config.isEnableCache()) {
            this.cache = Caffeine
                    .newBuilder()
                    .maximumSize(config.getCacheMaximumSize())
                    .expireAfterWrite(config.getCacheExpiredDuration())
                    .build();
        }
    }

    @Override
    public void saveFlow(FlowDef flowDef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateFlow(FlowDef flowDef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFlow(String name) {
        throw new UnsupportedOperationException();
    }

    public FlowDef getFlow(String name) {
        validate();
        return config.isEnableCache()
                ? cache.get(name, __ -> getFromServerPoint(name))
                : getFromServerPoint(name);
    }

    private FlowDef getFromServerPoint(String name) {

        httpAsyncClient.start();

        try {
            name = URLEncoder.encode(name,
                    Charset.defaultCharset().name());
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(uee);
        }

        String uriWithParams = config.getServerUri()
                + Delimiter.QUESTION_MARK
                + config.getNameKey()
                + Delimiter.EQUAL
                + name;

        HttpGet httpGet = new HttpGet(URI.create(uriWithParams));

        Future<HttpResponse> responseFuture =
                httpAsyncClient.execute(httpGet, EMPTY_CALLBACK);

        HttpResponse response;

        try {
            response = responseFuture.get();
        } catch (InterruptedException e) {
            log.error("HTTP request {} metadata from {} interrupted",
                    name, httpGet.getURI());
            return null;
        } catch (ExecutionException e) {
            log.error(String.format("HTTP request %s metadata from %s exception",
                    name, httpGet.getURI()), e);
            return null;
        }

        if (200 == response.getStatusLine().getStatusCode()) {
            try {
                if (config.isWrapped()) {
                    String entityStr = EntityUtils.toString(response.getEntity());

                    Response<FlowDef> wrapped = JsonUtils.readValue(
                            entityStr,
                            new TypeReference<Response<FlowDef>>() {
                            });

                    if (wrapped == null || !wrapped.isSuccess()) {
                        log.error("HTTP request {} metadata from {} fail, result: {}",
                                name, httpGet.getURI(), entityStr);
                        return null;
                    }

                    return wrapped.getResult();
                } else {
                    return JsonUtils.readValue(
                            EntityUtils.toString(response.getEntity()),
                            FlowDef.class);
                }
            } catch (Exception e) {
                log.error(String.format(
                        "HTTP request %s metadata from %s read content exception",
                        name, httpGet.getURI()), e);
                return null;
            }
        } else {
            String serverReason = null;

            if (response.getEntity() != null) {
                try {
                    serverReason = EntityUtils.toString(response.getEntity());
                } catch (Exception ignored) {
                }
            }

            log.error("HTTP request {} metadata from {} error, " +
                            "statusCode: {} reasonPhrase: {}, serverReason: ({})",
                    name, httpGet.getURI(),
                    response.getStatusLine().getStatusCode(),
                    response.getStatusLine().getReasonPhrase(),
                    serverReason);
        }

        return null;
    }

    @Override
    public void destroy() {
        if (httpAsyncClient != null) {
            try {
                httpAsyncClient.close();
            } catch (IOException e) {
                log.error("Close HTTP task client exception", e);
            }
        }
    }

    private void validate() {
        if (httpAsyncClient == null) {
            throw new IllegalStateException("Uninitialized");
        }
    }

    @Data
    static class Response<T> implements Serializable {

        private static final long serialVersionUID = -4493608083661886867L;

        private boolean success;

        private T result;

        private String code;

        private String message;
    }
}
