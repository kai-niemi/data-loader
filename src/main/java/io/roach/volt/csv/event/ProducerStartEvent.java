package io.roach.volt.csv.event;

import io.roach.volt.csv.model.Table;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Event published when a CSV producer for a table is scheduled to start.
 */
public class ProducerStartEvent extends AbstractEvent {
    private final boolean quitOnCompletion;

    private final CountDownLatch startLatch;

    public ProducerStartEvent(Table table, Path path, CountDownLatch startLatch, boolean quitOnCompletion) {
        super(table, path);
        this.startLatch = startLatch;
        this.quitOnCompletion = quitOnCompletion;
    }

    public boolean isQuitOnCompletion() {
        return quitOnCompletion;
    }

    public CountDownLatch getStartLatch() {
        return startLatch;
    }
}
