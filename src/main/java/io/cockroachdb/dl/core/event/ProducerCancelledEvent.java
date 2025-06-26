package io.cockroachdb.dl.core.event;

import java.nio.file.Path;

import io.cockroachdb.dl.core.model.Table;

/**
 * Event published when all CSV producers should cancel further processing.
 */
public class ProducerCancelledEvent extends AbstractTableEvent {
    public ProducerCancelledEvent(Table table, Path path) {
        super(table, path);
    }
}
