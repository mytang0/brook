package xyz.mytang0.brook.core.metadata;

import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.core.exception.FlowException;
import xyz.mytang0.brook.spi.metadata.MetadataService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static xyz.mytang0.brook.core.exception.FlowErrorCode.FLOW_UNSUPPORTED_OPERATION;

@Slf4j
public class FileMetadataService implements MetadataService {

    private static final String FLOW_CONFIG_SUFFIX = ".json";

    private static final String FLOW_CONFIG_PATTERN = "META-INF/flows";

    private final LoadingCache<String, Optional<FlowDef>> flowDefCache;

    public FileMetadataService() {
        this.flowDefCache = Caffeine
                .newBuilder()
                .maximumSize(512)
                .build(this::getFlowOptional);
    }

    @Override
    public void saveFlow(FlowDef flowDef) {
        Optional<FlowDef> newOptional = Optional.of(flowDef);
        flowDefCache.put(flowDef.getName(), newOptional);
    }

    @Override
    public void updateFlow(FlowDef flowDef) {
        Optional<FlowDef> newOptional = Optional.of(flowDef);
        flowDefCache.put(flowDef.getName(), newOptional);
    }

    @Override
    public void deleteFlow(String name) {
        throw new FlowException(FLOW_UNSUPPORTED_OPERATION);
    }

    @Override
    public FlowDef getFlow(String name) {
        return Objects.requireNonNull(flowDefCache.get(name))
                .orElse(null);
    }

    private Optional<FlowDef> getFlowOptional(String name) {
        String jsonConfig = FileFlowConfigUtils.readFlowConfig(name);
        return StringUtils.isNotBlank(jsonConfig)
                ? Optional.ofNullable(JsonUtils.readValue(jsonConfig, FlowDef.class))
                : Optional.empty();
    }

    @SuppressWarnings("all")
    static class FileFlowConfigUtils {

        private static volatile boolean initOk = false;

        private static final Map<String, URL> fileUrlMap = new HashMap<>();

        private static final Map<String, String> fileContentMap = new HashMap<>();

        static String readFlowConfig(String name) {

            if (!initOk) {
                synchronized (FileFlowConfigUtils.class) {
                    if (!initOk) {
                        loadFlowConfigUrls();
                        initOk = true;
                    }
                }
            }

            String content = fileContentMap.get(name);

            if (content == null) {

                URL url = fileUrlMap.get(name);

                if (url != null) {
                    try (InputStream inputStream = url.openStream()) {
                        content = readContent(inputStream);
                    } catch (Throwable throwable) {
                        log.error("Read flow config from url:(" + url + ") fail", throwable);
                    }
                }
            }

            if (StringUtils.isNotBlank(content)) {
                try {
                    JsonUtils.readValue(content, new TypeReference<Map<String, Object>>() {
                    });
                } catch (Throwable throwable) {
                    log.error(String.format(
                            "Invalid flow config file, name: (%s), content: (%s)",
                            name, content));
                }
            }

            return content;
        }

        static void loadFlowConfigUrls() {
            try {
                Enumeration<URL> flowConfigs =
                        Thread.currentThread()
                                .getContextClassLoader()
                                .getResources(FLOW_CONFIG_PATTERN);
                while (flowConfigs.hasMoreElements()) {
                    URL resource = flowConfigs.nextElement();
                    String protocol = resource.getProtocol();
                    if ("file".equals(protocol)) {
                        scanFile(resource);
                    } else if ("jar".equals(protocol)) {
                        scanJar(resource);
                    }
                }
            } catch (Throwable throwable) {
                log.error("Load flow config urls fail", throwable);
            }
        }

        static void scanJar(URL resource) throws Exception {
            JarURLConnection jarURLConnection =
                    (JarURLConnection) resource.openConnection();

            try (JarFile jarFile = jarURLConnection.getJarFile()) {

                Enumeration<JarEntry> entries = jarFile.entries();

                while (entries.hasMoreElements()) {

                    JarEntry jarEntry = entries.nextElement();

                    String name = jarEntry.getName();

                    if (StringUtils.startsWith(name, FLOW_CONFIG_PATTERN)
                            && StringUtils.endsWith(name, FLOW_CONFIG_SUFFIX)) {

                        try {
                            fileContentMap.put(getFlowName(name),
                                    readContent(jarFile.getInputStream(jarEntry)));
                        } catch (Exception e) {
                            log.error(String.format(
                                    "Read flow config from (%s) fail", name), e);
                        }
                    }
                }
            }
        }

        static void scanFile(URL resource) throws Exception {
            scanFile(resource.getPath());
        }

        static void scanFile(String filePath) throws Exception {
            File directory = new File(filePath);
            File[] listFiles = directory.listFiles();
            if (listFiles != null) {
                for (File file : listFiles) {
                    if (file.isDirectory()) {
                        scanFile(file.toURI().toURL());
                    } else {
                        String path = file.getPath();
                        if (StringUtils.endsWith(path, FLOW_CONFIG_SUFFIX)) {
                            fileUrlMap.put(getFlowName(path), file.toURI().toURL());
                        }
                    }
                }
            }
        }

        static String getFlowName(String fileName) {
            int begin = Math.max(0, StringUtils.lastIndexOf(fileName, "/"));
            int end = fileName.indexOf(FLOW_CONFIG_SUFFIX);
            if (begin < end) {
                return fileName.substring(begin + 1, end);
            }
            throw new IllegalArgumentException("Invalid flow config file: (" + fileName + ")");
        }

        static String readContent(InputStream inputStream) throws Exception {
            String content = null;
            try (BufferedReader reader =
                         new BufferedReader(
                                 new InputStreamReader(
                                         inputStream, StandardCharsets.UTF_8))) {
                String line;
                StringBuilder configBuilder = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    configBuilder.append(line);
                }
                content = configBuilder.toString();
            }
            return content;
        }
    }
}
