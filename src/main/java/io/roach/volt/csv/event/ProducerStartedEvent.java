package io.roach.volt.csv.event;

import io.roach.volt.csv.model.Table;

import java.nio.file.Path;

/**
 * Event published when a CSV producer for a table has started.
 */
public class ProducerStartedEvent extends AbstractEvent {
    public ProducerStartedEvent(Table table, Path path) {
        super(table, path);
    }

    public boolean isBounded() {
        return getTable().getFinalCount() > 0;
    }
}
