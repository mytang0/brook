package xyz.mytang0.brook.common.context;

import java.util.concurrent.Callable;

/**
 * A Runnable/Callable wrapper that propagates FlowContext across threads.
 * <p>
 * Captures a {@link FlowContextSnapshot} at creation time and applies it
 * to the executing thread before running the delegate. The snapshot is
 * cleared after execution to prevent ThreadLocal leaks.
 * <p>
 * Usage:
 * <pre>
 * executor.submit(FlowContextPropagatingRunnable.wrap(() -> {
 *     // FlowContext is available here
 *     doWork();
 * }));
 * </pre>
 */
public final class FlowContextPropagatingRunnable implements Runnable {

    private final FlowContextSnapshot snapshot;

    private final Runnable delegate;

    private FlowContextPropagatingRunnable(FlowContextSnapshot snapshot,
                                           Runnable delegate) {
        this.snapshot = snapshot;
        this.delegate = delegate;
    }

    /**
     * Wraps a Runnable with context propagation. Captures the current
     * thread's FlowContext at the time of wrapping.
     *
     * @param delegate the runnable to wrap
     * @return a context-propagating runnable
     */
    public static Runnable wrap(Runnable delegate) {
        return new FlowContextPropagatingRunnable(
                FlowContextSnapshot.capture(), delegate);
    }

    /**
     * Wraps a Runnable with a specific context snapshot.
     *
     * @param snapshot the context snapshot to propagate
     * @param delegate the runnable to wrap
     * @return a context-propagating runnable
     */
    public static Runnable wrap(FlowContextSnapshot snapshot, Runnable delegate) {
        return new FlowContextPropagatingRunnable(snapshot, delegate);
    }

    /**
     * Wraps a Callable with context propagation. Captures the current
     * thread's FlowContext at the time of wrapping.
     *
     * @param delegate the callable to wrap
     * @param <V>      the return type
     * @return a context-propagating callable
     */
    public static <V> Callable<V> wrap(Callable<V> delegate) {
        FlowContextSnapshot snapshot = FlowContextSnapshot.capture();
        return () -> {
            snapshot.apply();
            try {
                return delegate.call();
            } finally {
                snapshot.clear();
            }
        };
    }

    /**
     * Wraps a Callable with a specific context snapshot.
     *
     * @param snapshot the context snapshot to propagate
     * @param delegate the callable to wrap
     * @param <V>      the return type
     * @return a context-propagating callable
     */
    public static <V> Callable<V> wrap(FlowContextSnapshot snapshot,
                                       Callable<V> delegate) {
        return () -> {
            snapshot.apply();
            try {
                return delegate.call();
            } finally {
                snapshot.clear();
            }
        };
    }

    @Override
    public void run() {
        snapshot.apply();
        try {
            delegate.run();
        } finally {
            snapshot.clear();
        }
    }
}
