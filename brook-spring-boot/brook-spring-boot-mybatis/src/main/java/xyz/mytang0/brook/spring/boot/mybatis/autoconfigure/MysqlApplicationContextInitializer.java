package xyz.mytang0.brook.spring.boot.mybatis.autoconfigure;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import xyz.mytang0.brook.spring.boot.mybatis.constants.TableNameConstants;
import com.baomidou.mybatisplus.autoconfigure.MybatisPlusProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@Conditional(MysqlAutoConfiguration.EnableAnyCondition.class)
public class MysqlApplicationContextInitializer
        implements ApplicationContextInitializer<ConfigurableApplicationContext>,
        BeanPostProcessor,
        Ordered {

    private static final String PREFIX = "";

    private static final Map<String, String> dynamicTableNames = new HashMap<>();

    public void initialize(ConfigurableApplicationContext context) {
        ConfigurableEnvironment configurableEnvironment = context.getEnvironment();
        if (BooleanUtils.isTrue(configurableEnvironment.getProperty("brook.execution-dao.mysql.enabled", Boolean.class))) {
            Optional.of(configurableEnvironment.getProperty(
                            "brook.execution-dao.mysql.flow-table-name",
                            TableNameConstants.DEFAULT_FLOW_TABLE_NAME))
                    .ifPresent(tableName ->
                            dynamicTableNames.put(PREFIX + "executionDaoFlowTableName", tableName));

            Optional.of(configurableEnvironment.getProperty(
                            "brook.execution-dao.mysql.flow-pending-table-name",
                            TableNameConstants.DEFAULT_FLOW_PENDING_TABLE_NAME))
                    .ifPresent(tableName ->
                            dynamicTableNames.put(PREFIX + "executionDaoFlowPendingTableName", tableName));

            Optional.of(configurableEnvironment.getProperty(
                            "brook.execution-dao.mysql.task-table-name",
                            TableNameConstants.DEFAULT_TASK_TABLE_NAME))
                    .ifPresent(tableName ->
                            dynamicTableNames.put(PREFIX + "executionDaoTaskTableName", tableName));

            Optional.of(configurableEnvironment.getProperty(
                            "brook.execution-dao.mysql.task-pending-table-name",
                            TableNameConstants.DEFAULT_TASK_PENDING_TABLE_NAME))
                    .ifPresent(tableName ->
                            dynamicTableNames.put(PREFIX + "executionDaoTaskPendingTableName", tableName));
        }

        if (BooleanUtils.isTrue(configurableEnvironment.getProperty("brook.queue.mysql.enabled", Boolean.class))) {
            Optional.of(configurableEnvironment.getProperty(
                            "brook.queue.mysql.table-name",
                            TableNameConstants.DEFAULT_QUEUE_TABLE_NAME))
                    .ifPresent(tableName ->
                            dynamicTableNames.put(PREFIX + "queueTableName", tableName));
        }

        if (BooleanUtils.isTrue(configurableEnvironment.getProperty("brook.metadata.mysql.enabled", Boolean.class))) {
            Optional.of(configurableEnvironment.getProperty(
                            "brook.metadata.mysql.table-name",
                            TableNameConstants.DEFAULT_FLOW_DEF_TABLE_NAME))
                    .ifPresent(tableName ->
                            dynamicTableNames.put(PREFIX + "metadataTableName", tableName));
        }
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public Object postProcessAfterInitialization(@Nonnull Object bean,
                                                 @Nonnull String beanName) throws BeansException {
        if (bean instanceof MybatisPlusProperties) {
            MybatisPlusProperties mybatisPlusProperties = (MybatisPlusProperties) bean;
            MybatisConfiguration configuration = mybatisPlusProperties.getConfiguration();
            if (configuration == null) {
                mybatisPlusProperties.setConfiguration(
                        configuration = new MybatisConfiguration());
            }
            configuration.getVariables().putAll(dynamicTableNames);
        }
        return bean;
    }
}
