package io.cockroachdb.dlr.core.event;

import java.nio.file.Path;
import java.time.Duration;

import io.cockroachdb.dlr.core.model.Table;

/**
 * Event published when a CSV producer for a table completes successfully
 * without failure or graceful cancellation.
 */
public class ProducerCompletedEvent extends AbstractTableEvent {
    public ProducerCompletedEvent(Table table, Path path) {
        super(table, path);
    }

    private int rows;

    private Duration duration;

    public int getRows() {
        return rows;
    }

    public ProducerCompletedEvent setRows(int rows) {
        this.rows = rows;
        return this;
    }

    public Duration getDuration() {
        return duration;
    }

    public ProducerCompletedEvent setDuration(Duration duration) {
        this.duration = duration;
        return this;
    }

    public double calcRequestsPerSec() {
        return (double) rows / Math.max(1, duration.toMillis()) * 1000.0;
    }
}
