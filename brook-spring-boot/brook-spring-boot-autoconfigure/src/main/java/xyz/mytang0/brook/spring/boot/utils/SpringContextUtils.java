package xyz.mytang0.brook.spring.boot.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class SpringContextUtils implements ApplicationContextAware {

    private static ApplicationContext context;

    private static ApplicationContext parentContext;

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
        parentContext = applicationContext.getParent();
    }

    public static <T> T getBeanOfType(@NotNull Class<T> type) throws BeansException {
        T result = null;
        try {
            result = context.getBean(type);
        } catch (BeansException e) {
            if (Objects.nonNull(parentContext)) {
                result = parentContext.getBean(type);
            }
        }
        return result;
    }

    public static <T> Map<String, T> getBeansOfType(@Nullable Class<T> type) throws BeansException {
        Map<String, T> result = context.getBeansOfType(type);
        if (result.isEmpty() && Objects.nonNull(parentContext)) {
            result = parentContext.getBeansOfType(type);
        }
        return result;
    }

    public static <T> Map<String, T> getBeansOfType(
            @Nullable Class<T> type, boolean includeNonSingletons,
            boolean allowEagerInit)
            throws BeansException {
        Map<String, T> result = context.getBeansOfType(type, includeNonSingletons, allowEagerInit);
        if (result.isEmpty() && Objects.nonNull(parentContext)) {
            result = parentContext.getBeansOfType(type, includeNonSingletons, allowEagerInit);
        }
        return result;
    }

    public static <T> T getBean(
            String name,
            @Nullable Class<T> requiredType) throws BeansException {
        T result = null;
        try {
            result = context.getBean(name, requiredType);
        } catch (BeansException e) {
            if (Objects.nonNull(parentContext)) {
                result = parentContext.getBean(name, requiredType);
            }
        }
        return result;
    }

    public static String[] getBeanNamesForType(@Nullable Class<?> type) {
        String[] result = context.getBeanNamesForType(type);
        if (ArrayUtils.isEmpty(result) && Objects.nonNull(parentContext)) {
            result = parentContext.getBeanNamesForType(type);
        }
        return result;
    }

    public static Object getBean(String name) throws BeansException {
        Object result;
        try {
            result = context.getBean(name);
        } catch (BeansException e) {
            if (Objects.nonNull(parentContext)) {
                result = parentContext.getBean(name);
            } else {
                throw e;
            }
        }
        return result;
    }

    public static String[] getBeanDefinitionNames() {
        List<String> beanDefinitionNames = new ArrayList<>();
        String[] currentBeanDefinitionNames = context.getBeanDefinitionNames();
        if (ArrayUtils.isNotEmpty(currentBeanDefinitionNames)) {
            beanDefinitionNames.addAll(Arrays.asList(currentBeanDefinitionNames));
        }

        if (Objects.nonNull(parentContext)) {
            beanDefinitionNames.addAll(Arrays.asList(parentContext.getBeanDefinitionNames()));
        }
        return beanDefinitionNames.toArray(new String[0]);
    }

    public static List<Object> getAllBeans() {
        List<Object> allBeans = new ArrayList<>();

        String[] beanDefinitionNames = getBeanDefinitionNames();
        if (ArrayUtils.isNotEmpty(beanDefinitionNames)) {
            for (String beanDefinitionName : beanDefinitionNames) {
                allBeans.add(getBean(beanDefinitionName));
            }
        }

        return allBeans;
    }
}
