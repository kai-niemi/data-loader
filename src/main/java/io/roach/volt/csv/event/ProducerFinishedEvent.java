package io.roach.volt.csv.event;

import io.roach.volt.csv.model.Table;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Event published when a CSV producer for a table completed successfully
 * or was gracefully cancelled.
 */
public class ProducerFinishedEvent extends AbstractEvent {
    public ProducerFinishedEvent(Table table, Path path) {
        super(table, path);
    }

    private int rows;

    private Duration duration;

    private boolean cancelled;

    public boolean isCancelled() {
        return cancelled;
    }

    public ProducerFinishedEvent setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
        return this;
    }

    public int getRows() {
        return rows;
    }

    public ProducerFinishedEvent setRows(int rows) {
        this.rows = rows;
        return this;
    }

    public Duration getDuration() {
        return duration;
    }

    public ProducerFinishedEvent setDuration(Duration duration) {
        this.duration = duration;
        return this;
    }

    public double calcRequestsPerSec() {
        return  (double)
                rows /
                Math.max(1, duration.toMillis())
                * 1000.0;
    }
}
