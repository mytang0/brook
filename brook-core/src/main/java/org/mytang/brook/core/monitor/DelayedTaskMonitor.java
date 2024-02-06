package org.mytang.brook.core.monitor;


import org.mytang.brook.common.constants.Delimiter;
import org.mytang.brook.common.extension.ExtensionDirector;
import org.mytang.brook.common.extension.ExtensionLoader;
import org.mytang.brook.common.metadata.model.QueueMessage;
import org.mytang.brook.common.utils.ExceptionUtils;
import org.mytang.brook.common.utils.TimeUtils;
import org.mytang.brook.core.FlowExecutor;
import org.mytang.brook.core.exception.FlowException;
import org.mytang.brook.core.lock.FlowLockFacade;
import org.mytang.brook.core.utils.QueueUtils;
import org.mytang.brook.spi.executor.ExecutorFactory;
import org.mytang.brook.spi.queue.QueueService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.mytang.brook.core.utils.ThreadUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DelayedTaskMonitor {

    private static final Map<QueueService, DelayedTaskMonitor> monitors
            = new ConcurrentHashMap<>();

    private final String queueName;

    private final QueueService queueService;

    private final FlowExecutor<?> flowExecutor;

    private final FlowLockFacade flowLockFacade;

    private final ExecutorService taskExecutor;

    private final DelayedTaskMonitorProperties monitorProperties;

    private final ScheduledExecutorService unackedExecutor;

    private final ScheduledExecutorService pollingExecutor;

    private volatile boolean inited = false;

    public static void init(FlowExecutor<?> flowExecutor,
                            FlowLockFacade flowLockFacade,
                            DelayedTaskMonitorProperties monitorProperties) {

        if (CollectionUtils.isNotEmpty(monitorProperties.getQueueProtocols())) {
            ExtensionLoader<QueueService> extensionLoader =
                    ExtensionLoader.getExtensionLoader(QueueService.class);

            Thread asyncInitializer = new Thread(() ->
                    monitorProperties.getQueueProtocols().forEach(queueProtocol -> {
                        if (StringUtils.isNotBlank(queueProtocol)) {
                            while (!extensionLoader.getSupportedExtensions().contains(queueProtocol)) {
                                TimeUtils.sleepUninterruptedly(5, TimeUnit.SECONDS);
                            }
                            try {
                                QueueService queueService =
                                        ExtensionLoader.getExtension(QueueService.class, queueProtocol);
                                init(queueService, flowExecutor, flowLockFacade, monitorProperties);
                            } catch (Exception exception) {
                                log.error(String.format("Init %s delayed task monitor exception",
                                        queueProtocol), exception);
                            }
                        }
                    })
            );

            asyncInitializer.start();
        }
    }

    public static void init(QueueService queueService,
                            FlowExecutor<?> flowExecutor,
                            FlowLockFacade flowLockFacade,
                            DelayedTaskMonitorProperties monitorProperties) {
        if (!monitors.containsKey(queueService)) {
            synchronized (queueService) {
                if (!monitors.containsKey(queueService)) {
                    monitors.computeIfAbsent(queueService, __ ->
                            new DelayedTaskMonitor(
                                    queueService,
                                    flowExecutor,
                                    flowLockFacade,
                                    monitorProperties));
                }
            }
        }
    }

    public DelayedTaskMonitor(QueueService queueService,
                              FlowExecutor<?> flowExecutor,
                              FlowLockFacade flowLockFacade,
                              DelayedTaskMonitorProperties monitorProperties) {

        this.queueName = QueueUtils.getTaskDelayQueueName();
        this.queueService = queueService;
        this.flowExecutor = flowExecutor;
        this.flowLockFacade = flowLockFacade;
        this.taskExecutor = ExtensionDirector
                .getExtensionLoader(ExecutorFactory.class)
                .getDefaultExtension()
                .getExecutor(queueName);
        this.monitorProperties = monitorProperties;

        this.unackedExecutor = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.threadsNamed(queueService.getClass().getSimpleName()
                        + "-delayed-task-unacked-%d"));

        this.pollingExecutor = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.threadsNamed(queueService.getClass().getSimpleName()
                        + "-delayed-task-polling-%d"));

        this.initialize();

        log.info("Started queueName:{} queueService:{} delayed task monitor.",
                queueName, queueService);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                pollingExecutor.shutdown();
                unackedExecutor.shutdown();
            } catch (Exception ignored) {
                //
            }
        }));
    }

    private void initialize() {
        if (inited) {
            return;
        }
        inited = true;

        final Runnable unacked = () -> {
            try {
                long timePoint = TimeUtils.currentTimeMillis()
                        - monitorProperties.getUnackedTimeoutMs();
                queueService.unacked(queueName, timePoint);
            } catch (Throwable throwable) {
                log.error("Unacked exception", throwable);
            }
        };

        final Runnable poller = new Runnable() {
            @Override
            public void run() {
                long delayMs = 0;
                boolean locked = false;

                try {
                    if (!flowLockFacade.acquireLock(
                            DelayedTaskMonitor.class.getName(),
                            monitorProperties.getPollTimeoutMs())) {
                        return;
                    }

                    locked = true;

                    List<QueueMessage> messages =
                            queueService.poll(queueName,
                                    monitorProperties.getPollPerMaxSize(),
                                    monitorProperties.getPollTimeoutMs(),
                                    TimeUnit.MILLISECONDS);

                    if (CollectionUtils.isNotEmpty(messages)) {
                        messages.forEach(message -> {
                            if (message == null) {
                                return;
                            }
                            try {
                                taskExecutor.execute(() -> {
                                            try {
                                                log.info("Execute delayed task: {}", message.getId());
                                                String[] taskIdAndRetry = StringUtils.split(message.getId(), Delimiter.AT);
                                                if (taskIdAndRetry == null || taskIdAndRetry.length != 2) {
                                                    log.warn("Invalid delayed task: {}", message.getId());
                                                } else {
                                                    flowExecutor.executeTask(taskIdAndRetry[0]);
                                                }
                                                log.info("Execute delayed task: {} completion", message.getId());
                                                queueService.ack(queueName, message.getId());
                                            } catch (Throwable throwable) {
                                                if (throwable instanceof FlowException) {
                                                    log.error("Execute delayed task:({}) fail, reason:({}), postpone retry",
                                                            message.getId(), ExceptionUtils.getMessage(throwable));
                                                } else {
                                                    log.error("Execute delayed task:({}) exception, postpone retry",
                                                            message.getId(), throwable);
                                                }
                                                queueService.postpone(queueName, message);
                                            }
                                        }
                                );
                            } catch (Throwable throwable) {
                                log.error("Submit delayed task:({}) to executor exception", message.getId(), throwable);
                            }
                        });
                    }
                } catch (Throwable throwable) {
                    delayMs = monitorProperties.getExceptionRetryIntervalMs();
                    log.warn("Poll exception, retry after {} milliseconds", delayMs, throwable);
                } finally {

                    if (locked) {
                        try {
                            flowLockFacade.releaseLock(DelayedTaskMonitor.class.getName());
                        } catch (Exception ignored) {
                            //
                        }
                    }

                    pollingExecutor.schedule(this, delayMs, TimeUnit.MILLISECONDS);
                }
            }
        };

        unackedExecutor.scheduleAtFixedRate(unacked, 0,
                monitorProperties.getUnackedIntervalMs(), TimeUnit.MILLISECONDS);

        pollingExecutor.schedule(poller, 0, TimeUnit.MILLISECONDS);
    }
}
