package io.cockroachdb.dl.core.event;

import io.cockroachdb.dl.core.model.Table;

import java.nio.file.Path;

/**
 * Event published when a CSV producer for a table has started to produce items/rows.
 */
public class ProducerStartedEvent extends AbstractTableEvent {
    public ProducerStartedEvent(Table table, Path path) {
        super(table, path);
    }
}
