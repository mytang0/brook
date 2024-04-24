package xyz.mytang0.brook.spring.boot.autoconfigure;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import xyz.mytang0.brook.core.FlowExecutor;
import xyz.mytang0.brook.core.FlowTaskRegistry;
import xyz.mytang0.brook.core.service.FlowLogService;
import xyz.mytang0.brook.spi.task.FlowTask;

@Configuration
@ComponentScan("xyz.mytang0.brook.spring.boot")
@ConditionalOnProperty(name = "brook.enabled", havingValue = "true", matchIfMissing = true)
public class FlowAutoConfiguration {

    @Bean
    public FlowLogService flowLogService() {
        return new FlowLogService();
    }

    @Bean
    public FlowTaskRegistry<FlowTask> flowTaskRegistry() {
        return new FlowTaskRegistry<>();
    }

    @Bean
    public FlowExecutor<FlowTask> flowExecutor(FlowTaskRegistry<FlowTask> flowTaskRegistry) {
        return new FlowExecutor<>(flowTaskRegistry);
    }
}
