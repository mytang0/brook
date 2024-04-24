package xyz.mytang0.brook.common.utils;

import org.apache.commons.lang3.ClassUtils;

public abstract class ReflectUtils {

    public static boolean isPrimitives(Class<?> cls) {
        while (cls.isArray()) {
            cls = cls.getComponentType();
        }
        return isPrimitive(cls);
    }

    public static boolean isPrimitive(Class<?> cls) {
        return ClassUtils.isPrimitiveOrWrapper(cls);
    }

    public static boolean isJdkClass(Class<?> cls) {
        String packageName = cls.getPackage().getName();
        return packageName.startsWith("java.") ||
                packageName.startsWith("javax.") ||
                packageName.startsWith("com.sun.");
    }
}
