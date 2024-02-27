package xyz.mytang0.brook.core.service;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import java.nio.file.Paths;

import static xyz.mytang0.brook.common.constants.Delimiter.EMPTY;

public class FlowLogService {

    private final FlowLogProperties properties;

    public FlowLogService(FlowLogProperties properties) {
        this.properties = properties;
    }

    public String getLog(String id) {
        return properties.getRoot()
                + id + properties.getSuffix();
    }

    @Data
    public static class FlowLogProperties {

        private static final String LOG_SUFFIX = ".log";

        /**
         * After the flow turns on log output, the storage path of the log. The default is '$USER_DIR/logs'.
         */
        private String root = getDefaultRoot();

        /**
         * The log file suffix, default value is '.log'.
         */
        private String suffix = LOG_SUFFIX;

        static String getDefaultRoot() {
            String defaultRoot = SystemUtils.USER_DIR;
            if (StringUtils.isBlank(defaultRoot)) {
                defaultRoot = Paths.get(EMPTY)
                        .toAbsolutePath().toString();
            }
            if (StringUtils.isNotBlank(defaultRoot)
                    && defaultRoot.endsWith("/")) {
                defaultRoot = defaultRoot.substring(0, defaultRoot.length() - 1);
            }
            return defaultRoot + "/logs/";
        }
    }
}
