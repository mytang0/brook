package xyz.mytang0.brook.common.extension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.mytang0.brook.common.annotation.OrderComparator;
import xyz.mytang0.brook.common.extension.injector.ExtensionInjector;
import xyz.mytang0.brook.common.extension.loading.LoadingStrategy;
import xyz.mytang0.brook.common.utils.Holder;
import xyz.mytang0.brook.common.utils.ReflectUtils;
import xyz.mytang0.brook.common.utils.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.ServiceLoader.load;
import static java.util.stream.StreamSupport.stream;

public class ExtensionLoader<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtensionLoader.class);

    private static final LoadingStrategy[] STRATEGIES = loadLoadingStrategies();

    private static final Map<Class<?>, ExtensionLoader<?>> LOADERS = new ConcurrentHashMap<>();

    private final Class<T> clazz;

    private final Holder<Object> cachedSelectedInstance = new Holder<>();

    private final ClassLoader classLoader;

    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<>();

    private final Map<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<>();

    private final Map<Class<?>, Object> extensionInstances = new ConcurrentHashMap<>();

    private final AtomicBoolean destroyed = new AtomicBoolean();

    private String cachedDefaultName;

    /**
     * Instantiates a new Extension loader.
     *
     * @param clazz the clazz.
     */
    private ExtensionLoader(final Class<T> clazz, final ClassLoader cl) {
        this.clazz = clazz;
        this.classLoader = cl;
        if (clazz != ExtensionInjector.class) {
            ExtensionLoader<?> extensionLoader = LOADERS.get(ExtensionInjector.class);
            if (Objects.isNull(extensionLoader)) {
                getExtensionLoader(ExtensionInjector.class).getExtensionInstances();
            }
        }
    }

    private static LoadingStrategy[] loadLoadingStrategies() {
        return stream(load(LoadingStrategy.class).spliterator(), false)
                .sorted(OrderComparator.INSTANCE)
                .toArray(LoadingStrategy[]::new);
    }


    /**
     * Gets extension loader.
     *
     * @param <T>   the type parameter
     * @param clazz the clazz
     * @param cl    the cl
     * @return the extension loader.
     */
    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(final Class<T> clazz, final ClassLoader cl) {

        Objects.requireNonNull(clazz, "extension clazz is null");

        if (!clazz.isInterface()) {
            throw new IllegalArgumentException("extension clazz (" + clazz + ") is not interface!");
        }
        if (!clazz.isAnnotationPresent(SPI.class)) {
            throw new IllegalArgumentException("extension clazz (" + clazz + ") without @" + SPI.class + " Annotation");
        }
        ExtensionLoader<T> extensionLoader = (ExtensionLoader<T>) LOADERS.get(clazz);
        if (Objects.nonNull(extensionLoader)) {
            return extensionLoader;
        }
        LOADERS.putIfAbsent(clazz, new ExtensionLoader<>(clazz, cl));
        return (ExtensionLoader<T>) LOADERS.get(clazz);
    }


    /**
     * 直接获取想要的类实例
     *
     * @param clazz 接口的Class实例
     * @param name  SPI名称
     * @param <T>   泛型类型
     * @return 泛型实例
     */
    public static <T> T getExtension(final Class<T> clazz, String name) {
        return StringUtils.isEmpty(name)
                ? getExtensionLoader(clazz).getDefaultExtension()
                : getExtensionLoader(clazz).getExtension(name);
    }

    /**
     * Gets extension loader.
     *
     * @param <T>   the type parameter
     * @param clazz the clazz
     * @return the extension loader
     */
    public static <T> ExtensionLoader<T> getExtensionLoader(final Class<T> clazz) {
        return getExtensionLoader(clazz, ExtensionLoader.class.getClassLoader());
    }

    /**
     * Gets default spi class instance.
     * Default selection of instances marked with @Selected annotations
     *
     * @return the default spi class instance.
     */
    @SuppressWarnings("unchecked")
    public T getDefaultExtension() {
        checkDestroyed();
        getExtensionClasses();
        T instance = (T) cachedSelectedInstance.get();
        if (Objects.isNull(instance)) {
            synchronized (cachedSelectedInstance) {
                instance = (T) cachedSelectedInstance.get();
                if (Objects.isNull(instance)) {
                    instance = createSelectedExtension(cachedClasses.get());
                    cachedSelectedInstance.set(instance);
                }
            }
        }
        if (Objects.nonNull(instance)) {
            return instance;
        }

        if (StringUtils.isBlank(cachedDefaultName)) {
            return null;
        }
        return getExtension(cachedDefaultName);
    }

    /**
     * Gets spi class.
     *
     * @param name the name
     * @return the spi class instance.
     */
    @SuppressWarnings("unchecked")
    public T getExtension(final String name) {
        if (StringUtils.isBlank(name)) {
            throw new NullPointerException(
                    String.format("get spi name is null, spi class: %s", clazz));
        }
        checkDestroyed();
        Holder<Object> objectHolder = cachedInstances.get(name);
        if (Objects.isNull(objectHolder)) {
            cachedInstances.putIfAbsent(name, new Holder<>());
            objectHolder = cachedInstances.get(name);
        }
        T value = (T) objectHolder.get();
        if (Objects.isNull(value)) {
            synchronized (cachedInstances) {
                value = (T) objectHolder.get();
                if (Objects.isNull(value)) {
                    value = createExtension(name);
                    objectHolder.set(value);
                }
            }
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public T createSelectedExtension(Map<String, Class<?>> classMap) {
        T instance = null;
        for (Class<?> aClass : classMap.values()) {
            if (aClass.isAnnotationPresent(Selected.class)) {
                instance = (T) extensionInstances.get(aClass);
                if (Objects.isNull(instance)) {
                    try {
                        instance = newInstance(aClass);
                    } catch (InstantiationException | IllegalAccessException e) {
                        throw new IllegalStateException("Extension instance(class: "
                                + aClass + ")  could not be instantiated: " + e.getMessage(), e);
                    }
                }
            }
        }
        return instance;
    }


    /**
     * get all spi class spi.
     *
     * @return list. spi instances
     */
    @SuppressWarnings("unchecked")
    public List<T> getExtensionInstances() {
        checkDestroyed();
        Map<String, Class<?>> extensionClasses = getExtensionClasses();
        if (extensionClasses.isEmpty()) {
            return Collections.emptyList();
        }
        if (Objects.equals(extensionClasses.size(), cachedInstances.size())) {
            return (List<T>) this.cachedInstances.values()
                    .stream()
                    .map(Holder::get)
                    .sorted(OrderComparator.INSTANCE)
                    .collect(Collectors.toList());
        }
        // Dynamic Loading
        return extensionClasses.keySet()
                .stream()
                .map(this::getExtension)
                .sorted(OrderComparator.INSTANCE)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private T createExtension(final String name) {
        Class<?> aClass = getExtensionClasses().get(name);
        if (Objects.isNull(aClass)) {
            throw new IllegalStateException("Extension class not found(name: " + name + ")");
        }
        T instance = (T) extensionInstances.get(aClass);
        if (Objects.isNull(instance)) {
            try {
                instance = newInstance(aClass);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IllegalStateException("Extension instance(name: " + name + ", class: "
                        + aClass + ")  could not be instantiated: " + e.getMessage(), e);
            }
        }
        return instance;
    }

    @SuppressWarnings("unchecked")
    private T newInstance(Class<?> aClass) throws InstantiationException, IllegalAccessException {
        extensionInstances.putIfAbsent(aClass, aClass.newInstance());
        T instance = (T) extensionInstances.get(aClass);
        injectExtension(instance);
        return instance;
    }


    /**
     * Gets extension classes.
     *
     * @return the extension classes
     */
    private Map<String, Class<?>> getExtensionClasses() {
        Map<String, Class<?>> classes = cachedClasses.get();
        if (Objects.isNull(classes)) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                if (Objects.isNull(classes)) {
                    classes = loadExtensionClass();
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
    }

    public void addExtension(String name, Class<?> clazz, Object instance) {
        addExtensionClass(name, clazz);
        addExtensionInstance(name, clazz, instance);
    }

    public void addExtensionClass(String name, Class<?> clazz) {
        checkDestroyed();
        getExtensionClasses().put(name, clazz);
    }

    public void addExtensionInstance(String name, Class<?> clazz, Object instance) {
        checkDestroyed();
        // Replace
        if (isSelected(clazz)) {
            cachedDefaultName = name;
            cachedSelectedInstance.set(instance);
        }
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<>());
            holder = cachedInstances.get(name);
        }
        holder.set(instance);
    }

    private boolean isSelected(Class<?> clazz) {
        boolean selected = clazz.isAnnotationPresent(Selected.class);
        if (!selected) {
            Annotation[] annotations = clazz.getAnnotations();
            for (Annotation annotation : annotations) {
                Annotation[] parentAnnotations = annotation.annotationType().getAnnotations();
                for (Annotation parentAnnotation : parentAnnotations) {
                    if (parentAnnotation instanceof Selected) {
                        return true;
                    }
                }
            }
        }
        return selected;
    }

    private Map<String, Class<?>> loadExtensionClass() {
        SPI annotation = clazz.getAnnotation(SPI.class);
        if (Objects.nonNull(annotation)) {
            String value = annotation.value();
            if (StringUtils.isNotBlank(value)) {
                cachedDefaultName = value;
            }
        }
        Map<String, Class<?>> classes = new ConcurrentHashMap<>();
        loadDirectory(classes);
        return classes;
    }

    private void loadDirectory(final Map<String, Class<?>> classes) {
        for (LoadingStrategy directory : STRATEGIES) {
            String fileName = directory.directory() + clazz.getName();
            try {
                Enumeration<URL> urls = Objects.nonNull(this.classLoader)
                        ? classLoader.getResources(fileName)
                        : ClassLoader.getSystemResources(fileName);
                if (Objects.nonNull(urls)) {
                    while (urls.hasMoreElements()) {
                        URL url = urls.nextElement();
                        loadResources(classes, url);
                    }
                }
            } catch (IOException t) {
                LOG.error("load extension class error {}", fileName, t);
            }
        }
    }

    /**
     * 加载资源
     */
    private void loadResources(final Map<String, Class<?>> classes, final URL url) throws IOException {
        try (InputStream inputStream = url.openStream()) {
            Properties properties = new Properties();
            properties.load(inputStream);
            properties.forEach((k, v) -> {
                String name = (String) k;
                String classPath = (String) v;
                if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(classPath)) {
                    try {
                        loadClass(classes, name, classPath);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalStateException("load extension resources error", e);
                    }
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException("load extension resources error", e);
        }
    }

    private void loadClass(final Map<String, Class<?>> classes,
                           final String name, final String classPath) throws ClassNotFoundException {
        Class<?> subClass = Objects.nonNull(this.classLoader) ? Class.forName(classPath, true, this.classLoader) : Class.forName(classPath);
        if (!clazz.isAssignableFrom(subClass)) {
            throw new IllegalStateException("load extension resources error," + subClass + " subtype is not of " + clazz);
        }

        Class<?> oldClass = classes.get(name);
        if (Objects.isNull(oldClass)) {
            classes.put(name, subClass);
        } else if (!Objects.equals(oldClass, subClass)) {
            throw new IllegalStateException("load extension resources error,Duplicate class " + clazz.getName() + " name " + name + " on " + oldClass.getName() + " or " + subClass.getName());
        }
    }

    /**
     * inject
     */
    private T injectExtension(T instance) {
        try {
            for (Method method : instance.getClass().getMethods()) {
                if (!isSetter(method)) {
                    continue;
                }
                if (method.isAnnotationPresent(DisableInject.class)) {
                    continue;
                }
                Class<?> pt = method.getParameterTypes()[0];
                // 基本类型不注入
                if (ReflectUtils.isPrimitives(pt)) {
                    continue;
                }
                try {
                    String property = getSetterProperty(method);
                    Object object = getExtensionInjectInstance(pt, property);
                    if (object != null) {
                        method.invoke(instance, object);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to inject via method " + method.getName() + ": " + e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return instance;
    }

    @SuppressWarnings("unchecked")
    private Object getExtensionInjectInstance(Class<?> type, String name) {
        ExtensionLoader<ExtensionInjector> injectorLoader = (ExtensionLoader<ExtensionInjector>) LOADERS.get(ExtensionInjector.class);
        List<ExtensionInjector> injectorList = injectorLoader.getExtensionInstances();
        return injectorList.stream().map(injector -> injector.getInstance(type, name))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private void checkDestroyed() {
        if (destroyed.get()) {
            throw new IllegalStateException("ExtensionLoader is destroyed: " + clazz);
        }
    }

    public Set<String> getSupportedExtensions() {
        checkDestroyed();
        Map<String, Class<?>> classes = getExtensionClasses();
        return Collections.unmodifiableSet(new TreeSet<>(classes.keySet()));
    }

    private boolean isSetter(Method method) {
        return method.getName().startsWith("set")
                && method.getParameterTypes().length == 1
                && Modifier.isPublic(method.getModifiers());
    }

    private String getSetterProperty(Method method) {
        return method.getName().length() > 3 ? method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4) : "";
    }

    public void destroy() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        // destroy raw extension instance
        extensionInstances.forEach((type, instance) -> {
            if (instance instanceof Disposable) {
                Disposable disposable = (Disposable) instance;
                try {
                    disposable.destroy();
                } catch (Exception e) {
                    LOG.error("Error destroying extension " + disposable, e);
                }
            }
        });
        extensionInstances.clear();

        // destroy wrapped extension instance
        for (Holder<Object> holder : cachedInstances.values()) {
            Object wrappedInstance = holder.get();
            if (wrappedInstance instanceof Disposable) {
                Disposable disposable = (Disposable) wrappedInstance;
                try {
                    disposable.destroy();
                } catch (Exception e) {
                    LOG.error("Error destroying extension " + disposable, e);
                }
            }
        }
        cachedInstances.clear();
    }
}
