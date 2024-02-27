package xyz.mytang0.brook.common.metadata.enums;

public enum FlowStatus {
    RUNNING(false, false),
    COMPLETED(true, true),
    FAILED(true, false),
    TIMED_OUT(true, false),
    TERMINATED(true, false),
    PAUSED(false, true);

    private final boolean terminal;

    private final boolean successful;

    FlowStatus(boolean terminal, boolean successful) {
        this.terminal = terminal;
        this.successful = successful;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public boolean isRunning() {
        return this.equals(RUNNING);
    }

    public boolean isPaused() {
        return this.equals(PAUSED);
    }
}
