package io.roach.volt.csv.event;

public class ExitEvent {
    private final int exitCode;

    public ExitEvent(int exitCode) {
        this.exitCode = exitCode;
    }

    public int getExitCode() {
        return exitCode;
    }
}
