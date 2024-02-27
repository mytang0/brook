package xyz.mytang0.brook.core.callback;

import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.common.utils.StringUtils;
import xyz.mytang0.brook.spi.callback.TaskCallback;
import lombok.Data;
import org.apache.commons.lang3.ClassUtils;
import xyz.mytang0.brook.spi.callback.ClassTaskCallback;

import java.io.Serializable;
import java.util.Objects;

public final class DefaultClassTaskCallback implements TaskCallback {

    @Override
    public void onCreated(Object input, TaskInstance taskInstance) {
        Objects.requireNonNull(input, "Class callback input is null.");

        ClassInput classInput = JsonUtils.convertValue(input, ClassInput.class);
        classInput.validate();

        try {
            ClassTaskCallback classTaskCall
                    = newCallbackInstance(classInput);

            classTaskCall.onCreated(taskInstance);
        } catch (ClassNotFoundException cne) {
            throw new IllegalStateException(String.format(
                    "Class task callback implementation class-name:(%s) not found.",
                    classInput.getClassName()),
                    cne);
        } catch (InstantiationException | IllegalAccessException ne) {
            throw new IllegalStateException(String.format(
                    "Class task callback implementation class-name:(%s) new instance fail.",
                    classInput.getClassName()),
                    ne);
        }
    }

    @Override
    public void onTerminated(Object input, TaskInstance taskInstance) {
        Objects.requireNonNull(input, "Class callback input is null.");

        ClassInput classInput = JsonUtils.convertValue(input, ClassInput.class);
        classInput.validate();

        try {
            ClassTaskCallback classTaskCall
                    = newCallbackInstance(classInput);

            classTaskCall.onTerminated(taskInstance);
        } catch (ClassNotFoundException cne) {
            throw new IllegalStateException(String.format(
                    "Class task callback implementation class-name:(%s) not found.",
                    classInput.getClassName()),
                    cne);
        } catch (InstantiationException | IllegalAccessException ne) {
            throw new IllegalStateException(String.format(
                    "Class task callback implementation class-name:(%s) new instance fail.",
                    classInput.getClassName()),
                    ne);
        }
    }

    private ClassTaskCallback newCallbackInstance(ClassInput classInput)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        final Class<?> callbackClass = ClassUtils.getClass(
                ClassTaskCallback.class.getClassLoader(),
                classInput.getClassName()
        );

        if (!ClassUtils.isAssignable(callbackClass, ClassTaskCallback.class)) {
            throw new IllegalStateException(String.format(
                    "Class task callback implementation class-name:(%s)" +
                            " needs to implement the (%s) interface.",
                    classInput.getClassName(),
                    ClassTaskCallback.class.getName()));
        }

        return (ClassTaskCallback) callbackClass.newInstance();
    }

    @Data
    public static class ClassInput implements Serializable {

        private static final long serialVersionUID = -5832425226472985480L;

        private String className;

        public void validate() {
            if (StringUtils.isBlank(className)) {
                throw new IllegalArgumentException("Class callback class-name is null.");
            }
        }
    }
}
