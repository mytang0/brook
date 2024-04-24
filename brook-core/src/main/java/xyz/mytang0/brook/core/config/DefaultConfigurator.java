package xyz.mytang0.brook.core.config;

import xyz.mytang0.brook.common.constants.Delimiter;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.utils.FieldUtils;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.common.utils.MethodUtils;
import xyz.mytang0.brook.common.utils.ReflectUtils;
import xyz.mytang0.brook.spi.config.ConfigProperties;
import xyz.mytang0.brook.spi.config.ConfigSource;
import xyz.mytang0.brook.spi.config.ConfigValidator;
import xyz.mytang0.brook.spi.config.Configurator;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static xyz.mytang0.brook.common.utils.FieldUtils.setFieldValue;

public class DefaultConfigurator implements Configurator {

    private static final Map<Class<?>, Optional<Object>>
            INSTANCES = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getConfig(Class<T> type) {
        Optional<Object> optional = INSTANCES.get(type);
        if (optional == null) {
            optional = INSTANCES.computeIfAbsent(type,
                    __ -> Optional.ofNullable(bind(type)));
        }

        return (T) optional.orElse(null);
    }

    @Override
    public <T> T refreshConfig(Class<T> type) {
        INSTANCES.remove(type);
        return getConfig(type);
    }

    public <T> T bind(Class<T> type) {
        if (Modifier.isInterface(type.getModifiers())
                || Modifier.isAbstract(type.getModifiers())) {
            throw new IllegalArgumentException("Not instantiable!");
        }

        ConfigProperties annotation =
                type.getAnnotation(ConfigProperties.class);
        if (annotation == null) {
            throw new IllegalArgumentException(
                    String.format("Missing @%s on class '%s'.",
                            ConfigProperties.class.getSimpleName(),
                            type.getName()));
        }

        return bind(type, annotation.prefix(), annotation);
    }

    @SuppressWarnings("unchecked")
    private static <T> T bind(Class<T> type, String prefix, ConfigProperties annotation) {

        // Already sorted according to @Order.
        List<ConfigSource> configSources =
                ExtensionDirector
                        .getExtensionLoader(ConfigSource.class)
                        .getExtensionInstances();

        AtomicReference<T> instanceReference = new AtomicReference<>();

        if (!configSources.isEmpty()) {
            MethodUtils.getSetterMethods(type)
                    .forEach(setter -> {
                                Object value = getFieldValue(configSources, prefix, setter);
                                if (value != null) {
                                    if (instanceReference.get() == null) {
                                        instanceReference.set(newInstance(type));
                                    }
                                    setFieldValue(setter, instanceReference.get(), value);
                                }
                            }
                    );
        }

        // Validity verification.
        if (annotation != null) {
            Class<? extends ConfigValidator<T>> validator =
                    (Class<? extends ConfigValidator<T>>) annotation.validator();
            if (!ConfigValidator.NULL.class.isAssignableFrom(validator)) {
                Type generictype = validator.getGenericInterfaces()[0];
                if (generictype instanceof ParameterizedType) {
                    generictype = ((ParameterizedType) generictype)
                            .getActualTypeArguments()[0];
                    if (generictype instanceof Class
                            && type.isAssignableFrom((Class<?>) generictype)) {
                        validate(validator, instanceReference.get());
                    }
                }
            }
        }

        return instanceReference.get();
    }

    private static Object getFieldValue(List<ConfigSource> configSources, String prefix, Method setter) {

        String propertyName = extractPropertyName(setter);
        if (propertyName == null) {
            return null;
        }

        Class<?> type = setter.getParameterTypes()[0];

        ConfigProperties annotation =
                Optional.ofNullable(FieldUtils.getFieldByName(
                                setter.getDeclaringClass(), propertyName))
                        .map(field -> field.getAnnotation(ConfigProperties.class))
                        .orElse(null);

        Object value = null;

        if (annotation != null) {
            prefix = getConfigKey(prefix, annotation.prefix());
            value = bind(type, prefix, annotation);
        } else {
            propertyName = getConfigKey(prefix, propertyName);
            for (ConfigSource configSource : configSources) {
                if ((value = configSource.getProperty(propertyName)) != null) {
                    value = convertPropertyValue(value, type);
                    break;
                }
            }

            // Initialized from a collection with the same prefix.
            if (value == null
                    && !ReflectUtils.isPrimitives(type)
                    && !ReflectUtils.isJdkClass(type)) {

                value = bind(type, propertyName, null);
            }
        }

        return value;
    }

    private static String extractPropertyName(Method method) {
        String methodName = method.getName();
        String propertyName = methodName.substring(3);
        if (propertyName.isEmpty()) {
            return null;
        }
        char[] chars = propertyName.toCharArray();
        chars[0] = Character.toLowerCase(chars[0]);
        return new String(chars);
    }

    private static Object convertPropertyValue(Object value, Class<?> type) {
        try {
            return JsonUtils.convertValue(value, type);
        } catch (IllegalArgumentException e) {
            if (value instanceof String) {
                return JsonUtils.readValue((String) value, type);
            }
            throw e;
        }
    }

    private static String getConfigKey(String prefix, String property) {
        return prefix + Delimiter.DOT + property;
    }

    private static <T> void validate(
            Class<? extends ConfigValidator<T>> validator, T target) {

        ConfigValidator<T> instance = newInstance(validator);

        instance.validate(target);
    }

    @SuppressWarnings("unchecked")
    private static <T> T newInstance(Class<?> type) {
        try {
            return (T) type.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
