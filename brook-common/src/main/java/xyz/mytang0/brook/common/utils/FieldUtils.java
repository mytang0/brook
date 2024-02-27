package xyz.mytang0.brook.common.utils;

import com.fasterxml.jackson.core.type.TypeReference;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

public abstract class FieldUtils {

    /**
     * void(V).
     */
    public static final char JVM_VOID = 'V';

    /**
     * boolean(Z).
     */
    public static final char JVM_BOOLEAN = 'Z';

    /**
     * byte(B).
     */
    public static final char JVM_BYTE = 'B';

    /**
     * char(C).
     */
    public static final char JVM_CHAR = 'C';

    /**
     * double(D).
     */
    public static final char JVM_DOUBLE = 'D';

    /**
     * float(F).
     */
    public static final char JVM_FLOAT = 'F';

    /**
     * int(I).
     */
    public static final char JVM_INT = 'I';

    /**
     * long(J).
     */
    public static final char JVM_LONG = 'J';

    /**
     * short(S).
     */
    public static final char JVM_SHORT = 'S';

    public static String modifier(int mod) {
        if (Modifier.isPublic(mod)) {
            return "public";
        }
        if (Modifier.isProtected(mod)) {
            return "protected";
        }
        if (Modifier.isPrivate(mod)) {
            return "private";
        }
        return "";
    }

    public static String getName(Class<?> c) {
        if (c.isArray()) {
            StringBuilder sb = new StringBuilder();
            do {
                sb.append("[]");
                c = c.getComponentType();
            }
            while (c.isArray());

            return c.getName() + sb;
        }
        return c.getName();
    }

    public static String getDescWithoutMethodName(Method m) {
        StringBuilder ret = new StringBuilder();
        ret.append('(');
        Class<?>[] parameterTypes = m.getParameterTypes();
        for (Class<?> parameterType : parameterTypes) {
            ret.append(getDesc(parameterType));
        }
        ret.append(')').append(getDesc(m.getReturnType()));
        return ret.toString();
    }

    public static String getDesc(final Constructor<?> c) {
        StringBuilder ret = new StringBuilder("(");
        Class<?>[] parameterTypes = c.getParameterTypes();
        for (Class<?> parameterType : parameterTypes) {
            ret.append(getDesc(parameterType));
        }
        ret.append(')').append('V');
        return ret.toString();
    }

    public static String getDesc(Class<?> c) {
        StringBuilder ret = new StringBuilder();

        while (c.isArray()) {
            ret.append('[');
            c = c.getComponentType();
        }

        if (c.isPrimitive()) {
            String t = c.getName();
            switch (t) {
                case "void":
                    ret.append(JVM_VOID);
                    break;
                case "boolean":
                    ret.append(JVM_BOOLEAN);
                    break;
                case "byte":
                    ret.append(JVM_BYTE);
                    break;
                case "char":
                    ret.append(JVM_CHAR);
                    break;
                case "double":
                    ret.append(JVM_DOUBLE);
                    break;
                case "float":
                    ret.append(JVM_FLOAT);
                    break;
                case "int":
                    ret.append(JVM_INT);
                    break;
                case "long":
                    ret.append(JVM_LONG);
                    break;
                case "short":
                    ret.append(JVM_SHORT);
                    break;
            }
        } else {
            ret.append('L');
            ret.append(c.getName().replace('.', '/'));
            ret.append(';');
        }
        return ret.toString();
    }

    @SuppressWarnings("unchecked")
    public static <T> T getProperty(Object obj, String fieldName) {
        if (obj instanceof Map) {
            return (T) ((Map<?, ?>) obj).get(fieldName);
        } else if (obj == null) {
            return null;
        } else {
            try {
                return (T) getValueByName(obj, fieldName);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    public static Object getValueByName(Object obj, String fieldName)
            throws IllegalArgumentException, IllegalAccessException {
        Field field = getFieldByName(obj, fieldName);
        if (field == null) {
            return null;
        }
        if (!field.isAccessible()) {
            field.setAccessible(true);
        }
        return field.get(obj);
    }

    public static Field getFieldByName(Object obj, String fieldName) {
        Class<?> superClass = obj.getClass();

        while (superClass != Object.class) {
            try {
                return superClass.getDeclaredField(fieldName);
            } catch (NoSuchFieldException var4) {
                superClass = superClass.getSuperclass();
            }
        }
        return null;
    }

    public static Map<String, Object> objectToMap(Object fromValue) {
        return JsonUtils.convertValue(fromValue, new TypeReference<Map<String, Object>>() {
        });
    }
}