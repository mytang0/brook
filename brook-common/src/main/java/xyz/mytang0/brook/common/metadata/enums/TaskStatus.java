package xyz.mytang0.brook.common.metadata.enums;

public enum TaskStatus {
    SCHEDULED(false, true, true),
    IN_PROGRESS(false, true, true),
    CANCELED(true, false, false),
    FAILED(true, false, true),
    COMPLETED(true, true, true),
    TIMED_OUT(true, false, true),
    SKIPPED(true, true, false),
    RETRIED(true, false, true),
    HANGED(true, true, false),
    FAILED_WITH_TERMINAL_ERROR(true, false, false);

    private final boolean terminal;

    private final boolean successful;

    private final boolean retryable;

    TaskStatus(boolean terminal, boolean successful, boolean retryable) {
        this.terminal = terminal;
        this.successful = successful;
        this.retryable = retryable;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public boolean isFinished() {
        return terminal && successful;
    }

    public boolean isCompleted() {
        return this.equals(COMPLETED);
    }

    public boolean isRetried() {
        return this.equals(RETRIED);
    }

    public boolean isSkipped() {
        return this.equals(SKIPPED);
    }

    public boolean isScheduled() {
        return this.equals(SCHEDULED);
    }

    public boolean isHanged() {
        return this.equals(HANGED);
    }

    public boolean isUnsuccessfullyTerminated() {
        return isTerminal() && !isSuccessful();
    }
}