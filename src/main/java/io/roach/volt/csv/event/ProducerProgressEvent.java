package io.roach.volt.csv.event;

import io.roach.volt.csv.model.Table;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

/**
 * Event published with CSV progress details.
 */
public class ProducerProgressEvent extends AbstractTableEvent {
    public ProducerProgressEvent(Table table, Path path) {
        super(table, path);
    }

    private long position;

    private long total;

    private Instant startTime;

    private String label;

    public ProducerProgressEvent setStartTime(Instant startTime) {
        this.startTime = startTime;
        return this;
    }

    public ProducerProgressEvent setLabel(String label) {
        this.label = label;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public long getPosition() {
        return position;
    }

    public ProducerProgressEvent setPosition(long position) {
        this.position = position;
        return this;
    }

    public long getTotal() {
        return total;
    }

    public ProducerProgressEvent setTotal(long total) {
        this.total = total;
        return this;
    }

    public Duration getElapsedTime() {
        return Duration.between(startTime, Instant.now());
    }

    public double getRequestsPerSec() {
        return (double)
                position /
                Math.max(1, getElapsedTime().toMillis())
                * 1000.0;
    }

    public long remainingMillis() {
        double rps = getRequestsPerSec();
        return rps > 0 ? (long) ((total - position) / rps * 1000) : 0;
    }
}
