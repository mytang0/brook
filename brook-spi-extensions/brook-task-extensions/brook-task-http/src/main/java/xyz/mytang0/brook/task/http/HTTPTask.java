package xyz.mytang0.brook.task.http;

import org.apache.commons.collections4.MapUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.constants.Delimiter;
import xyz.mytang0.brook.common.extension.Disposable;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.ExceptionUtils;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.common.utils.StringUtils;
import xyz.mytang0.brook.common.utils.token.TokenHandler;
import xyz.mytang0.brook.common.utils.token.TokenParser;
import xyz.mytang0.brook.spi.config.ConfiguratorFacade;
import xyz.mytang0.brook.spi.task.FlowTask;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.http.entity.ContentType.APPLICATION_FORM_URLENCODED;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.BODY;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.CHARSET;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.HEADERS;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.METHOD;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.PARAMS;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.REASON_PHRASE;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.STATUS_CODE;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.URI;
import static xyz.mytang0.brook.task.http.HTTPTask.Options.VARIABLES;

public class HTTPTask implements FlowTask, Disposable {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("HTTP")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Call the HTTP interface.");

    private static final Map<String, Future<HttpResponse>>
            requestMap = new ConcurrentHashMap<>();

    private static final String VAR_OPEN_TOKEN = "{";

    private static final String VAR_CLOSE_TOKEN = "}";

    private static final FutureCallback<HttpResponse> EMPTY_CALLBACK =
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

    private final HTTPTaskConfig config;

    private volatile boolean uninitialized = true;

    private volatile CloseableHttpAsyncClient httpAsyncClient;

    public HTTPTask() {
        this.config = ConfiguratorFacade
                .getConfig(HTTPTaskConfig.class);
    }

    @Override
    public ConfigOption<?> catalog() {
        return CATALOG;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URI);
        options.add(METHOD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BODY);
        options.add(HEADERS);
        options.add(PARAMS);
        options.add(VARIABLES);
        options.add(CHARSET);
        return options;
    }

    @Override
    public boolean execute(TaskInstance taskInstance) {
        // Running.
        if (requestMap.containsKey(taskInstance.getTaskId())) {
            return false;
        }
        try {
            request(taskInstance);
        } catch (Throwable throwable) {
            taskInstance.setStatus(TaskStatus.FAILED);
            taskInstance.setReasonForNotCompleting(
                    ExceptionUtils.getMessage(throwable));
        } finally {
            requestMap.remove(taskInstance.getTaskId());
        }
        return true;
    }

    @Override
    public void cancel(final TaskInstance taskInstance) {
        Optional.ofNullable(requestMap.get(taskInstance.getTaskId()))
                .ifPresent(request -> {
                    try {
                        request.cancel(Boolean.TRUE);
                    } catch (Exception ignored) {
                    }
                });
        taskInstance.setStatus(TaskStatus.CANCELED);
    }

    @SuppressWarnings("unchecked")
    private void request(TaskInstance taskInstance) throws Throwable {
        initialize();

        Configuration input = taskInstance.getInputConfiguration();

        String uri = input.get(URI);
        String charset = input.get(CHARSET);
        MethodEnum method = input.get(METHOD);
        Object body = input.get(BODY);
        Map<String, String> headers = input.get(HEADERS);
        Map<String, Object> params = input.get(PARAMS);
        Map<String, Object> variables = input.get(VARIABLES);

        // Content type.
        String headContentType =
                MapUtils.getString(headers,
                        HttpHeaders.CONTENT_TYPE);

        ContentType contentType =
                StringUtils.isNotBlank(headContentType)
                        ? ContentType.parse(headContentType)
                        : APPLICATION_JSON;

        if (contentType.getCharset() == null) {
            contentType = contentType.withCharset(charset);
        }

        // Replace path variables.
        if (MapUtils.isNotEmpty(variables)) {
            uri = new TokenParser(
                    VAR_OPEN_TOKEN,
                    VAR_CLOSE_TOKEN,
                    new VariablesTokenHandler(variables)
            ).parse(uri);
        }

        // Build uri params.
        if (MapUtils.isNotEmpty(params)) {
            uri += buildParams(params, contentType.getCharset().name());
        }

        HttpUriRequest httpUriRequest;

        switch (method) {
            case PUT:
                httpUriRequest = new HttpPut(uri);
                ((HttpPut) httpUriRequest).setEntity(
                        buildEntity(contentType, body));
                break;
            case POST:
                httpUriRequest = new HttpPost(uri);
                ((HttpPost) httpUriRequest).setEntity(
                        buildEntity(contentType, body));
                break;
            case GET:
                httpUriRequest = new HttpGet(uri);
                break;
            case DELETE:
                httpUriRequest = new HttpDelete(uri);
                break;
            case OPTIONS:
                httpUriRequest = new HttpOptions(uri);
                break;
            case HEAD:
                httpUriRequest = new HttpHead(uri);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported HTTP method: %s",
                                method));
        }

        // Set headers.
        if (MapUtils.isNotEmpty(headers)) {
            headers.remove(HttpHeaders.CONTENT_TYPE);
            headers.forEach(httpUriRequest::addHeader);
        }
        httpUriRequest.addHeader(
                HttpHeaders.CONTENT_TYPE, contentType.toString());

        final FutureCallback<HttpResponse> futureCallback =
                new FutureCallback<HttpResponse>() {

                    @Override
                    public void completed(HttpResponse result) {
                        try {
                            process(result);
                        } catch (Throwable throwable) {
                            taskInstance.setStatus(TaskStatus.FAILED);
                            taskInstance.setReasonForNotCompleting(
                                    throwable.getLocalizedMessage()
                            );
                        }
                    }

                    @Override
                    public void failed(Exception e) {
                        taskInstance.setStatus(TaskStatus.FAILED);
                        taskInstance.setReasonForNotCompleting(
                                e.getLocalizedMessage()
                        );
                    }

                    @Override
                    public void cancelled() {
                        taskInstance.setStatus(TaskStatus.CANCELED);
                    }

                    private void process(HttpResponse result) throws Throwable {
                        int statusCode = result.getStatusLine().getStatusCode();
                        Map<String, Object> response = new HashMap<>();
                        response.put(STATUS_CODE.key(), statusCode);
                        response.put(REASON_PHRASE.key(),
                                result.getStatusLine().getReasonPhrase());

                        String body = null;
                        if (result.getEntity() != null) {
                            body = EntityUtils.toString(result.getEntity());
                        }

                        if (statusCode > 199 && statusCode < 300) {

                            // Parse headers.
                            Map<String, String> headers = new HashMap<>();
                            for (Header header : result.getAllHeaders()) {
                                headers.put(header.getName(), header.getValue());
                            }
                            response.put(HEADERS.key(), headers);

                            // Parse body.
                            Optional.ofNullable(body).ifPresent(str -> {
                                        if (StringUtils.isBlank(str)) {
                                            response.put(BODY.key(), str);
                                        } else if (str.charAt(0) != '{'
                                                && str.charAt(0) != '[') {
                                            response.put(BODY.key(), str);
                                        } else {
                                            response.put(BODY.key(), JsonUtils.parse(str));
                                        }
                                    }
                            );

                            taskInstance.setStatus(TaskStatus.COMPLETED);
                        } else {
                            taskInstance.setReasonForNotCompleting(
                                    Optional.ofNullable(body)
                                            .orElse("No response from the remote service")
                            );

                            taskInstance.setStatus(TaskStatus.FAILED);
                        }
                        taskInstance.setOutput(response);
                    }
                };

        httpAsyncClient.start();

        Future<HttpResponse> responseFuture = httpAsyncClient.execute(httpUriRequest, EMPTY_CALLBACK);

        requestMap.putIfAbsent(taskInstance.getTaskId(), responseFuture);

        long timeoutMs = Optional.ofNullable(taskInstance.getTaskDef().getControlDef())
                .map(TaskDef.ControlDef::getTimeoutMs)
                .orElse(0L);

        try {
            HttpResponse httpResponse;
            if (0 < timeoutMs) {
                httpResponse = responseFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
            } else {
                httpResponse = responseFuture.get();
            }
            futureCallback.completed(httpResponse);
        } catch (CancellationException ce) {
            futureCallback.cancelled();
        } catch (Exception e) {
            futureCallback.failed(e);
        }
    }

    private String buildParams(Map<String, Object> params, String charset) {
        StringBuilder uriParams = new StringBuilder();
        uriParams.append(Delimiter.QUESTION_MARK);
        params.forEach((param, value) -> {
            if (value instanceof String) {
                try {
                    value = URLEncoder.encode((String) value, charset);
                } catch (UnsupportedEncodingException uee) {
                    throw new RuntimeException(uee);
                }
            }
            uriParams
                    .append(param)
                    .append(Delimiter.EQUAL)
                    .append(value)
                    .append(Delimiter.AND);
        });
        uriParams.setLength(uriParams.length() - 1);
        return uriParams.toString();
    }

    @SuppressWarnings("unchecked")
    private HttpEntity buildEntity(ContentType contentType,
                                   Object body) {

        String mimeType = contentType.getMimeType();
        Charset charset = contentType.getCharset();

        if (APPLICATION_JSON.getMimeType().equals(mimeType)) {

            return new StringEntity(JsonUtils.toJsonString(body), charset);

        } else if (APPLICATION_FORM_URLENCODED.getMimeType().equals(mimeType)) {

            if (body instanceof String) {

                return new StringEntity(body.toString(), charset);

            } else if (body instanceof Map) {

                List<NameValuePair> params = new ArrayList<>();
                for (Map.Entry<String, String> entry :
                        ((Map<String, String>) body).entrySet()) {
                    params.add(new BasicNameValuePair(
                            entry.getKey(), entry.getValue()));
                }

                return new UrlEncodedFormEntity(params, charset);
            }

            throw new IllegalArgumentException("Illegal body");
        } else {

            return new StringEntity(JsonUtils.toJsonString(body), charset);
        }
    }

    @Override
    public void destroy() {
        if (httpAsyncClient != null) {
            try {
                httpAsyncClient.close();
            } catch (Exception ignored) {
            }
        }

        try {
            requestMap.forEach((taskId, future) -> {
                if (!future.isDone()
                        && !future.isCancelled()) {
                    future.cancel(true);
                }
            });
        } catch (Exception ignored) {
        }

        requestMap.clear();
    }

    private void initialize() {
        if (uninitialized) {
            synchronized (this) {
                if (uninitialized) {
                    HttpAsyncClientBuilder httpAsyncClientBuilder
                            = HttpAsyncClients.custom();

                    httpAsyncClientBuilder.setKeepAliveStrategy(
                                    new DefaultConnectionKeepAliveStrategy() {
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
                            );

                    Optional.ofNullable(config).ifPresent(conf -> {
                        Optional.ofNullable(conf.getClientConfig())
                                .ifPresent(clientConfig -> {
                                    httpAsyncClientBuilder
                                            .setMaxConnTotal(clientConfig.getMaxConnTotal())
                                            .setMaxConnPerRoute(clientConfig.getMaxConnPerRoute())
                                            .setDefaultRequestConfig(
                                                    RequestConfig.custom()
                                                            .setSocketTimeout(clientConfig.getSocketTimeout())
                                                            .setConnectTimeout(clientConfig.getConnectTimeout())
                                                            .setConnectionRequestTimeout(clientConfig.getConnectionRequestTimeout())
                                                            .build()
                                            );
                                });

                        Optional.ofNullable(conf.getTaskConfig())
                                .ifPresent(taskConfig -> {

                                });
                    });

                    httpAsyncClient = httpAsyncClientBuilder.build();

                    uninitialized = false;
                }
            }
        }
        httpAsyncClient.start();
    }

    static class VariablesTokenHandler implements TokenHandler {

        private final Map<String, Object> variables;

        VariablesTokenHandler(Map<String, Object> variables) {
            this.variables = variables;
        }

        @Override
        public String handleToken(String var) {
            return Optional.ofNullable(variables.get(var))
                    .map(Objects::toString)
                    .orElse(null);
        }
    }

    public static class Options {

        public static final ConfigOption<String> URI = ConfigOptions
                .key("uri")
                .stringType()
                .noDefaultValue()
                .withDescription("The HTTP URI.");

        public static final ConfigOption<MethodEnum> METHOD = ConfigOptions
                .key("method")
                .enumType(MethodEnum.class)
                .noDefaultValue()
                .withDescription("The HTTP method.");

        public static final ConfigOption<Object> BODY = ConfigOptions
                .key("body")
                .classType(Object.class)
                .noDefaultValue()
                .withDescription("The HTTP body.");

        public static final ConfigOption<Map<String, String>> HEADERS = ConfigOptions
                .key("headers")
                .mapType()
                .noDefaultValue()
                .withDescription("The HTTP headers.");

        @SuppressWarnings("rawtypes")
        public static final ConfigOption<Map> PARAMS = ConfigOptions
                .key("params")
                .classType(Map.class)
                .noDefaultValue()
                .withDescription("The HTTP request params.");

        @SuppressWarnings("rawtypes")
        public static final ConfigOption<Map> VARIABLES = ConfigOptions
                .key("variables")
                .classType(Map.class)
                .noDefaultValue()
                .withDescription("The HTTP path variables.");

        public static final ConfigOption<String> CHARSET = ConfigOptions
                .key("charset")
                .stringType()
                .defaultValue(StandardCharsets.UTF_8.name())
                .withDescription("The HTTP Accept-Charset.");

        public static final ConfigOption<Integer> STATUS_CODE = ConfigOptions
                .key("statusCode")
                .intType()
                .noDefaultValue()
                .withDescription("The HTTP response status-code.");

        public static final ConfigOption<String> REASON_PHRASE = ConfigOptions
                .key("reasonPhrase")
                .stringType()
                .noDefaultValue()
                .withDescription("The HTTP reason-phrase.");
    }

    protected enum MethodEnum {
        PUT,
        POST,
        GET,
        DELETE,
        OPTIONS,
        HEAD
    }
}
