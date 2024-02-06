package org.mytang.brook.common.utils;

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
}
