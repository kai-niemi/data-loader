package io.cockroachdb.volt.csv.event;

import io.cockroachdb.volt.csv.model.Table;

import java.nio.file.Path;

/**
 * Event published when a CSV producer for a table failed to
 * start or finished ungracefully.
 */
public class ProducerFailedEvent extends AbstractTableEvent {
    private Throwable cause;

    public ProducerFailedEvent(Table table, Path path) {
        super(table, path);
    }

    public Throwable getCause() {
        return cause;
    }

    public ProducerFailedEvent setCause(Throwable cause) {
        this.cause = cause;
        return this;
    }
}
