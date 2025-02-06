package xyz.mytang0.brook.core.utils;

import xyz.mytang0.brook.common.constants.Delimiter;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.extension.Extension;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.core.constants.FlowConstants;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

@Slf4j
public abstract class ParameterUtils {

    private static final JsonProvider jsonProvider = new FlowJsonProvider();

    private static final MappingProvider mappingProvider = new JacksonMappingProvider();


    @SuppressWarnings({"unchecked"})
    public static Object getFlowInput(final FlowDef flowDef,
                                      final Object input,
                                      final Extension extension) {
        assert flowDef != null;
        Object inputDef = flowDef.getInput();
        if (inputDef == null) {
            return input;
        } else if (inputDef instanceof Map) {
            return paramsMapping(externalContext(input, extension),
                    (Map<String, Object>) inputDef);
        } else if (inputDef instanceof List) {
            return paramsMapping(externalContext(input, extension),
                    (List<?>) inputDef);
        } else if (inputDef instanceof String) {
            return paramsMapping(externalContext(input, extension),
                    (String) inputDef);
        }
        return input;
    }

    public static Object getFlowOutput(final FlowInstance flowInstance) {
        assert flowInstance.getFlowDef() != null;

        Object outputDef = flowInstance.getFlowDef().getOutput();
        if (outputDef == null) {
            return Optional.ofNullable(flowInstance.getOutput())
                    .orElseGet(() ->
                            Optional.ofNullable(flowInstance.getTaskInstances())
                                    .filter(CollectionUtils::isNotEmpty)
                                    .map(instances -> {
                                                for (int index = instances.size() - 1;
                                                     0 <= index; index--) {
                                                    TaskInstance instance =
                                                            instances.get(index);
                                                    if (!instance.isHanging()) {
                                                        return instance.getOutput();
                                                    }
                                                }
                                                return null;
                                            }
                                    )
                                    .orElse(null)
                    );
        }

        return getMappingValue(flowInstance, outputDef);
    }

    public static Object getTaskInput(final FlowInstance flowInstance,
                                      final TaskDef taskDef) {
        assert taskDef != null;

        return getMappingValue(flowInstance, taskDef.getInput());
    }

    @SuppressWarnings("unchecked")
    public static Object getTaskOutput(final TaskInstance taskInstance) {

        assert taskInstance.getTaskDef() != null;

        Object outputDef = taskInstance.getTaskDef().getOutput();

        if (outputDef == null) {
            return taskInstance.getOutput();
        } else if (outputDef instanceof Map) {
            return paramsMapping(taskContext(taskInstance),
                    (Map<String, Object>) outputDef);
        } else if (outputDef instanceof List) {
            return paramsMapping(taskContext(taskInstance),
                    (List<?>) outputDef);
        } else if (outputDef instanceof String) {
            return paramsMapping(taskContext(taskInstance),
                    (String) outputDef);
        } else {
            return taskInstance.getOutput();
        }
    }

    public static Object getTaskInput(final TaskInstance taskInstance,
                                      final TaskDef taskDef) {
        assert taskDef != null;

        return getMappingValue(taskInstance, taskDef.getInput());
    }

    @SuppressWarnings("unchecked")
    public static Object getMappingValue(final FlowInstance flowInstance,
                                         final Object mappingDef) {
        if (mappingDef instanceof Map) {
            return paramsMapping(flowContext(flowInstance),
                    (Map<String, Object>) mappingDef);
        } else if (mappingDef instanceof List) {
            return paramsMapping(flowContext(flowInstance),
                    (List<?>) mappingDef);
        } else if (mappingDef instanceof String) {
            return paramsMapping(flowContext(flowInstance),
                    (String) mappingDef);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static Object getMappingValue(final TaskInstance taskInstance,
                                         final Object mappingDef) {
        if (mappingDef instanceof Map) {
            return paramsMapping(taskContext(taskInstance),
                    (Map<String, Object>) mappingDef);
        } else if (mappingDef instanceof List) {
            return paramsMapping(taskContext(taskInstance),
                    (List<?>) mappingDef);
        } else if (mappingDef instanceof String) {
            return paramsMapping(taskContext(taskInstance),
                    (String) mappingDef);
        }
        return null;
    }

    public static Object formatObject(final FlowInstance flowInstance,
                                      final String format) {
        return paramsMapping(flowContext(flowInstance), format);
    }

    public static Object formatObject(final TaskInstance taskInstance,
                                      final String format) {
        return paramsMapping(taskContext(taskInstance), format);
    }

    private static Map<String, Object> paramsMapping(final Map<String, Object> context,
                                                     final Map<String, Object> input) {

        Map<String, Object> inputParams;

        if (input != null) {
            inputParams = clone(input);
        } else {
            inputParams = new HashMap<>();
        }

        DocumentContext documentContext = JsonPath.parse(context, parseConfiguration());

        return replace(inputParams, documentContext);
    }

    private static List<?> paramsMapping(final Map<String, Object> context,
                                         final List<?> input) {

        List<?> inputParams;

        if (input != null) {
            inputParams = clone(input);
        } else {
            inputParams = Collections.emptyList();
        }

        DocumentContext documentContext = JsonPath.parse(context, parseConfiguration());

        return replaceList(inputParams, documentContext);
    }

    private static Object paramsMapping(final Map<String, Object> context,
                                        final String input) {

        DocumentContext documentContext = JsonPath.parse(context, parseConfiguration());

        return replaceVariables(input, documentContext);
    }

    public static Map<String, Object> clone(final Map<String, Object> input) {
        try {
            byte[] bytes = JsonUtils.writeValueAsBytes(input);
            return JsonUtils.readValue(bytes, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to clone input params", e);
        }
    }

    public static List<?> clone(final List<?> input) {
        try {
            byte[] bytes = JsonUtils.writeValueAsBytes(input);
            return JsonUtils.readValue(bytes, new TypeReference<List<Object>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to clone input params", e);
        }
    }

    public static Map<String, Object> replace(final Map<String, Object> input, final Object json) {
        Object doc;
        if (json instanceof String) {
            doc = JsonPath.parse(json.toString());
        } else {
            doc = json;
        }
        DocumentContext documentContext = JsonPath.parse(doc, parseConfiguration());
        return replace(input, documentContext);
    }

    public static Object replace(String paramString) {
        DocumentContext documentContext = JsonPath.parse(Collections.emptyMap(), parseConfiguration());
        return replaceVariables(paramString, documentContext);
    }

    private static Configuration parseConfiguration() {
        return Configuration
                .builder()
                .jsonProvider(jsonProvider)
                .mappingProvider(mappingProvider)
                .options(Option.SUPPRESS_EXCEPTIONS)
                .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
                .build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> replace(final Map<String, Object> input,
                                               final DocumentContext documentContext) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            Object newValue;
            Object value = entry.getValue();
            if (value instanceof String) {
                newValue = replaceVariables(value.toString(), documentContext);
            } else if (value instanceof Map) {
                newValue = replace((Map<String, Object>) value, documentContext);
            } else if (value instanceof List) {
                newValue = replaceList((List<?>) value, documentContext);
            } else {
                newValue = value;
            }
            result.put(entry.getKey(), newValue);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> replaceList(final List<?> values, final DocumentContext io) {
        List<Object> replacedList = new LinkedList<>();
        for (Object value : values) {
            if (value instanceof String) {
                Object replaced = replaceVariables(value.toString(), io);
                replacedList.add(replaced);
            } else if (value instanceof Map) {
                Object replaced = replace((Map<String, Object>) value, io);
                replacedList.add(replaced);
            } else if (value instanceof List) {
                Object replaced = replaceList((List<?>) value, io);
                replacedList.add(replaced);
            } else {
                replacedList.add(value);
            }
        }
        return replacedList;
    }

    private static Object replaceVariables(String paramString, final DocumentContext documentContext) {
        String[] values = paramString.split("(?=(?<!\\$)\\$\\{)|(?<=})");
        Object[] convertedValues = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            convertedValues[i] = values[i];
            if (values[i].startsWith("${") && values[i].endsWith("}")) {
                String paramPath = values[i].substring(2, values[i].length() - 1);
                // if the paramPath is blank, meaning no value in between ${ and }
                // like ${}, ${  } etc, set the value to empty string
                if (StringUtils.isBlank(paramPath)) {
                    convertedValues[i] = Delimiter.EMPTY;
                    continue;
                }
                try {
                    convertedValues[i] = documentContext.read(paramPath);
                } catch (Exception e) {
                    log.warn("Error reading documentContext for paramPath: {}", paramPath, e);
                    convertedValues[i] = null;
                }
            } else if (values[i].contains("$${")) {
                convertedValues[i] = values[i].replaceAll("\\$\\$\\{", "\\${");
            }
        }

        if (convertedValues.length > 1) {
            StringBuilder retBuilder = new StringBuilder();
            for (Object convertedValue : convertedValues) {
                retBuilder.append(Optional.ofNullable(convertedValue).orElse(Delimiter.EMPTY));
            }
            return retBuilder.toString();
        } else {
            return convertedValues[0];
        }
    }

    public static Map<String, Object> flowContext(final FlowInstance flowInstance) {
        final Map<String, Object> context = new HashMap<>();

        if (flowInstance == null) {
            return context;
        }

        Map<String, Object> flowContext = new HashMap<>();
        flowContext.put(FlowConstants.ID, flowInstance.getFlowId());
        flowContext.put(FlowConstants.DEF, flowInstance.getFlowDef());
        flowContext.put(FlowConstants.STATUS, flowInstance.getStatus());
        flowContext.put(FlowConstants.INPUT, flowInstance.getInput());
        flowContext.put(FlowConstants.OUTPUT, flowInstance.getOutput());
        flowContext.put(FlowConstants.CREATOR, flowInstance.getCreator());
        flowContext.put(FlowConstants.EXTENSION, flowInstance.getExtension());
        flowContext.put(FlowConstants.INSTANCE, flowInstance);
        context.put(FlowConstants.FLOW, flowContext);

        if (CollectionUtils.isNotEmpty(flowInstance.getTaskInstances())) {
            flowInstance.getTaskInstances().forEach(taskInstance ->
                    context.put(taskInstance.getTaskDef().getName(), taskContext(taskInstance))
            );
        }

        return context;
    }

    private static Map<String, Object> externalContext(final Object external,
                                                       final Extension extension) {
        Map<String, Object> context = new HashMap<>();
        context.put(FlowConstants.INPUT, external);
        context.put(FlowConstants.EXTENSION, extension);
        return context;
    }

    public static Map<String, Object> taskContext(final TaskInstance taskInstance) {
        Map<String, Object> taskContext = new HashMap<>();

        if (taskInstance == null) {
            return taskContext;
        }

        taskContext.put(FlowConstants.ID, taskInstance.getTaskId());
        taskContext.put(FlowConstants.DEF, taskInstance.getTaskDef());
        taskContext.put(FlowConstants.STATUS, taskInstance.getStatus());
        taskContext.put(FlowConstants.INPUT, taskInstance.getInput());
        taskContext.put(FlowConstants.OUTPUT, taskInstance.getOutput());
        taskContext.put(FlowConstants.EXTENSION, taskInstance.getExtension());
        taskContext.put(FlowConstants.INSTANCE, taskInstance);

        return taskContext;
    }


    private static class FlowJsonProvider extends JacksonJsonProvider {

        private static final ObjectMapper defaultObjectMapper =
                new ObjectMapper()
                        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

        private static final ObjectReader defaultObjectReader =
                defaultObjectMapper.reader().forType(Object.class);

        public FlowJsonProvider() {
            super(defaultObjectMapper, defaultObjectReader);
        }

        @Override
        public boolean isMap(Object obj) {
            if (obj instanceof Map) {
                return true;
            }
            Class<?> clazz = obj.getClass();
            if (ClassUtils.isPrimitiveOrWrapper(clazz)) {
                return false;
            }
            return !clazz.getName().startsWith("java.");
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Object getMapValue(Object obj, String key) {
            Map m;
            if (obj instanceof Map) {
                m = (Map) obj;
            } else {
                m = objectMapper.convertValue(obj, new TypeReference<Map<String, Object>>() {
                });
            }
            if (!m.containsKey(key)) {
                return JsonProvider.UNDEFINED;
            } else {
                return m.get(key);
            }
        }
    }
}
