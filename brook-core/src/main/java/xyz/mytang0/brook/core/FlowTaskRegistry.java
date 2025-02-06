package xyz.mytang0.brook.core;

import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.core.exception.FlowErrorCode;
import xyz.mytang0.brook.core.exception.FlowException;
import xyz.mytang0.brook.spi.task.FlowTask;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class FlowTaskRegistry<T extends FlowTask> {

    private final Class<T> taskInterface;

    private final Map<String, T> taskRegistry = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public FlowTaskRegistry() {
        Type supperClass = getClass().getGenericSuperclass();
        if (supperClass instanceof Class) {
            this.taskInterface = (Class<T>) FlowTask.class;
        } else {
            this.taskInterface = (Class<T>)
                    ((ParameterizedType) supperClass).getActualTypeArguments()[0];
        }
    }

    public void register(List<T> flowTasks) {
        if (flowTasks != null) {
            flowTasks.forEach(this::register);
        }
    }

    public void register(T flowTask) {
        if (!taskRegistry.containsKey(flowTask.getType())) {
            taskRegistry.putIfAbsent(flowTask.getType(), flowTask);
        }
    }

    public void forceRegister(T flowTask) {
        taskRegistry.put(flowTask.getType(), flowTask);
    }

    public T getFlowTask(String type) {
        return Optional.ofNullable(getAndInitialize(type))
                .orElseThrow(() ->
                        new FlowException(FlowErrorCode.FLOW_UNSUPPORTED_TASK,
                                String.format("Unsupported task type '%s'", type)));
    }

    public T getFlowTaskNoException(String type) {
        return getAndInitialize(type);
    }

    private T getAndInitialize(String type) {
        T t = taskRegistry.get(type);
        if (t == null) {
            synchronized (this) {
                t = taskRegistry.get(type);
                if (t == null) {
                    List<T> instances = ExtensionDirector.getExtensionLoader(taskInterface)
                            .getExtensionInstances();

                    instances.forEach(instance -> {
                                try {
                                    String taskType = instance.getType();
                                    taskRegistry.putIfAbsent(taskType, instance);
                                } catch (Exception exception) {
                                    log.error("Registry [{}] task fail",
                                            instance.getClass(), exception);
                                }
                            }
                    );
                    return taskRegistry.get(type);
                }
            }
        }
        return t;
    }

    public List<T> getFlowTasks() {
        return new ArrayList<>(taskRegistry.values());
    }
}
