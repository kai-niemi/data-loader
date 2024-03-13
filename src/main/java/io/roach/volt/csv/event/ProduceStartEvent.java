package io.roach.volt.csv.event;

import io.roach.volt.csv.model.Table;

import java.nio.file.Path;

/**
 * Event published when a CSV producer for a table can start.
 */
public class ProduceStartEvent extends AbstractEvent {
    private final boolean quitOnCompletion;

    public ProduceStartEvent(Table table, Path path, boolean quitOnCompletion) {
        super(table, path);
        this.quitOnCompletion = quitOnCompletion;
    }

    public boolean isQuitOnCompletion() {
        return quitOnCompletion;
    }
}
