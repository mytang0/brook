package xyz.mytang0.brook.spring.boot.mybatis.autoconfigure;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "xyz.mytang0.brook.spring.boot.mybatis")
@MapperScan(basePackages = "xyz.mytang0.brook.spring.boot.mybatis.mapper.**")
@Conditional(MysqlAutoConfiguration.EnableAnyCondition.class)
public class MysqlAutoConfiguration {

    @ConfigurationProperties("brook.metadata.mysql")
    public MysqlMetadataProperties mysqlMetadataProperties() {
        return new MysqlMetadataProperties();
    }

    @ConfigurationProperties("brook.queue.mysql")
    public MysqlQueueProperties mysqlQueueProperties() {
        return new MysqlQueueProperties();
    }

    @ConfigurationProperties("brook.execution-dao.mysql")
    public MysqlExecutionProperties mysqlExecutionProperties() {
        return new MysqlExecutionProperties();
    }

    public static class EnableAnyCondition extends AnyNestedCondition {

        public EnableAnyCondition() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty(name = "brook.metadata.mysql.enabled", havingValue = "true")
        static class EnableMysqlMetadata {
        }

        @ConditionalOnProperty(name = "brook.queue.mysql.enabled", havingValue = "true")
        static class EnableMysqlQueue {
        }

        @ConditionalOnProperty(name = "brook.execution-dao.mysql.enabled", havingValue = "true")
        static class EnableMysqlExecution {
        }
    }
}
