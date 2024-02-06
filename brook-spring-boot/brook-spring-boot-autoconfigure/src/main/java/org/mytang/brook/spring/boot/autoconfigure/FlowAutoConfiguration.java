package org.mytang.brook.spring.boot.autoconfigure;

import org.mytang.brook.core.FlowExecutor;
import org.mytang.brook.core.FlowTaskRegistry;
import org.mytang.brook.core.execution.ExecutionProperties;
import org.mytang.brook.core.lock.FlowLockFacade;
import org.mytang.brook.core.lock.LockProperties;
import org.mytang.brook.core.metadata.MetadataProperties;
import org.mytang.brook.core.monitor.DelayedTaskMonitorProperties;
import org.mytang.brook.core.queue.QueueProperties;
import org.mytang.brook.core.service.FlowLogService;
import org.mytang.brook.spi.task.FlowTask;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan("org.mytang.brook.spring.boot")
@ConditionalOnProperty(name = "brook.enabled", havingValue = "true", matchIfMissing = true)
public class FlowAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "brook.lock")
    public LockProperties flowLockProperties() {
        return new LockProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "brook.log")
    public FlowLogService.FlowLogProperties flowLogProperties() {
        return new FlowLogService.FlowLogProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "brook.queue")
    public QueueProperties queueProperties() {
        return new QueueProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "brook.execution-dao")
    public ExecutionProperties executionProperties() {
        return new ExecutionProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "brook.metadata")
    public MetadataProperties metadataProperties() {
        return new MetadataProperties();
    }

    @Bean
    @ConfigurationProperties(prefix = "brook.delayed.task.monitor")
    public DelayedTaskMonitorProperties delayedTaskMonitorProperties() {
        return new DelayedTaskMonitorProperties();
    }

    @Bean
    public FlowLockFacade flowLockFacade(LockProperties properties) {
        return new FlowLockFacade(properties);
    }

    @Bean
    public FlowLogService flowLogService(FlowLogService.FlowLogProperties properties) {
        return new FlowLogService(properties);
    }

    @Bean
    public FlowTaskRegistry<FlowTask> flowTaskRegistry() {
        return new FlowTaskRegistry<>();
    }

    @Bean
    public FlowExecutor<FlowTask> flowExecutor(FlowLockFacade flowLockFacade,
                                               FlowTaskRegistry<FlowTask> flowTaskRegistry,
                                               QueueProperties queueProperties,
                                               MetadataProperties metadataProperties,
                                               ExecutionProperties executionProperties,
                                               DelayedTaskMonitorProperties delayedTaskMonitorProperties) {
        return new FlowExecutor<>(
                flowLockFacade,
                flowTaskRegistry,
                queueProperties,
                metadataProperties,
                executionProperties,
                delayedTaskMonitorProperties);
    }
}
