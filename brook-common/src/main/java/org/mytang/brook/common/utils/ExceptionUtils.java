package org.mytang.brook.common.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;

public abstract class ExceptionUtils {

    private static final int STACK_TRACE_MAX_LENGTH = 300;

    public static String getMessage(final Throwable throwable) {
        String message = throwable instanceof InvocationTargetException
                ? throwable.getCause().getLocalizedMessage()
                : throwable.getLocalizedMessage();

        if (StringUtils.isBlank(message)) {
            try (StringWriter stringWriter = new StringWriter()) {
                throwable.printStackTrace(new PrintWriter(stringWriter));
                StringBuffer stringBuffer = stringWriter.getBuffer();
                if (STACK_TRACE_MAX_LENGTH < stringBuffer.length()) {
                    stringBuffer.setLength(STACK_TRACE_MAX_LENGTH);
                }
                message = stringWriter.toString();
            } catch (IOException ignored) {
            }
        }

        return message;
    }

    public static Throwable getRootCause(Throwable throwable) {
        Throwable slowPointer = throwable;

        Throwable cause;
        for (boolean advanceSlowPointer = false;
             (cause = throwable.getCause()) != null;
             advanceSlowPointer = !advanceSlowPointer) {

            throwable = cause;
            if (cause == slowPointer) {
                throw new IllegalArgumentException("Loop in causal chain detected.", cause);
            }

            if (advanceSlowPointer) {
                slowPointer = slowPointer.getCause();
            }
        }

        return throwable;
    }
}
