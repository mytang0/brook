package org.mytang.brook.common.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

public abstract class MethodUtils {

    public static Method getAccessibleMethod(final Class<?> cls,
                                             final String methodName,
                                             final Class<?>... parameterTypes) {
        Method method = null;
        try {
            method = cls.getMethod(methodName, parameterTypes);
        } catch (final NoSuchMethodException e) {
            if (parameterTypes.length == 0) {
                Method[] methods = cls.getMethods();
                for (Method pending : methods) {
                    if (pending.getName().equals(methodName)) {
                        method = pending;
                        break;
                    }
                }
            }
        }
        return getAccessibleMethod(method);
    }

    public static Method getAccessibleMethod(Method method) {
        if (!(method != null
                && Modifier.isPublic(method.getModifiers())
                && !method.isSynthetic())) {
            return null;
        }
        final Class<?> cls = method.getDeclaringClass();
        if (Modifier.isPublic(cls.getModifiers())) {
            return method;
        }
        final String methodName = method.getName();
        final Class<?>[] parameterTypes = method.getParameterTypes();

        method = getAccessibleMethodFromInterfaceNest(cls, methodName, parameterTypes);

        // Check the superclass chain
        if (method == null) {
            method = getAccessibleMethodFromSuperclass(cls, methodName, parameterTypes);
        }
        return method;
    }

    private static Method getAccessibleMethodFromInterfaceNest(Class<?> cls,
                                                               final String methodName,
                                                               final Class<?>... parameterTypes) {
        // Search up the superclass chain
        for (; cls != null; cls = cls.getSuperclass()) {

            // Check the implemented interfaces of the parent class
            final Class<?>[] interfaces = cls.getInterfaces();
            for (final Class<?> anInterface : interfaces) {
                // Is this interface public?
                if (!Modifier.isPublic(anInterface.getModifiers())) {
                    continue;
                }
                // Does the method exist on this interface?
                try {
                    return anInterface.getDeclaredMethod(methodName,
                            parameterTypes);
                } catch (final NoSuchMethodException e) {
                    /*
                     * Swallow, if no method is found after the loop then this
                     * method returns null.
                     */
                }
                // Recursively check our parent interfaces
                final Method method = getAccessibleMethodFromInterfaceNest(anInterface,
                        methodName, parameterTypes);
                if (method != null) {
                    return method;
                }
            }
        }
        return null;
    }

    private static Method getAccessibleMethodFromSuperclass(final Class<?> cls,
                                                            final String methodName,
                                                            final Class<?>... parameterTypes) {
        Class<?> parentClass = cls.getSuperclass();
        while (parentClass != null) {
            if (Modifier.isPublic(parentClass.getModifiers())) {
                try {
                    return parentClass.getMethod(methodName, parameterTypes);
                } catch (final NoSuchMethodException e) {
                    return null;
                }
            }
            parentClass = parentClass.getSuperclass();
        }
        return null;
    }

    public static List<Method> getMethodsListWithAnnotation(final Class<?> cls,
                                                            final Class<? extends Annotation> annotationCls) {
        return getMethodsListWithAnnotation(cls, annotationCls, false, false);
    }

    public static List<Method> getMethodsListWithAnnotation(final Class<?> cls,
                                                            final Class<? extends Annotation> annotationCls,
                                                            final boolean searchSupers,
                                                            final boolean ignoreAccess) {
        if (cls == null) {
            throw new IllegalArgumentException("The class must not be null");
        }
        if (annotationCls == null) {
            throw new IllegalArgumentException("The annotation class must not be null");
        }
        final List<Class<?>> classes = searchSupers
                ? getAllSuperclassesAndInterfaces(cls)
                : new ArrayList<>();
        classes.add(0, cls);
        final List<Method> annotatedMethods = new ArrayList<>();
        for (final Class<?> clazz : classes) {
            final Method[] methods = ignoreAccess
                    ? clazz.getDeclaredMethods()
                    : clazz.getMethods();
            for (final Method method : methods) {
                if (method.getAnnotation(annotationCls) != null) {
                    annotatedMethods.add(method);
                }
            }
        }
        return annotatedMethods;
    }

    private static List<Class<?>> getAllSuperclassesAndInterfaces(final Class<?> cls) {
        if (cls == null) {
            return null;
        }

        final List<Class<?>> allSuperClassesAndInterfaces = new ArrayList<>();
        final List<Class<?>> allSuperclasses = getAllSuperclasses(cls);
        int superClassIndex = 0;
        final List<Class<?>> allInterfaces = getAllInterfaces(cls);
        int interfaceIndex = 0;
        while (interfaceIndex < allInterfaces.size() ||
                superClassIndex < allSuperclasses.size()) {
            Class<?> clazz;
            if (interfaceIndex >= allInterfaces.size()) {
                clazz = allSuperclasses.get(superClassIndex++);
            } else if (superClassIndex >= allSuperclasses.size()) {
                clazz = allInterfaces.get(interfaceIndex++);
            } else if (interfaceIndex < superClassIndex) {
                clazz = allInterfaces.get(interfaceIndex++);
            } else if (superClassIndex < interfaceIndex) {
                clazz = allSuperclasses.get(superClassIndex++);
            } else {
                clazz = allInterfaces.get(interfaceIndex++);
            }
            allSuperClassesAndInterfaces.add(clazz);
        }
        return allSuperClassesAndInterfaces;
    }

    public static List<Class<?>> getAllSuperclasses(final Class<?> cls) {
        if (cls == null) {
            return null;
        }
        final List<Class<?>> classes = new ArrayList<>();
        Class<?> superclass = cls.getSuperclass();
        while (superclass != null) {
            classes.add(superclass);
            superclass = superclass.getSuperclass();
        }
        return classes;
    }

    public static List<Class<?>> getAllInterfaces(final Class<?> cls) {
        if (cls == null) {
            return null;
        }

        final LinkedHashSet<Class<?>> interfacesFound = new LinkedHashSet<>();
        getAllInterfaces(cls, interfacesFound);

        return new ArrayList<>(interfacesFound);
    }

    private static void getAllInterfaces(Class<?> cls,
                                         final HashSet<Class<?>> interfacesFound) {
        while (cls != null) {
            final Class<?>[] interfaces = cls.getInterfaces();

            for (final Class<?> i : interfaces) {
                if (interfacesFound.add(i)) {
                    getAllInterfaces(i, interfacesFound);
                }
            }

            cls = cls.getSuperclass();
        }
    }
}
