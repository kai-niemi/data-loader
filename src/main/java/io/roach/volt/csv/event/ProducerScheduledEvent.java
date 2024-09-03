package io.roach.volt.csv.event;

import io.roach.volt.csv.model.Table;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Event published when a CSV producer for a table can initialize and
 * is scheduled to start after a latch is released.
 */
public class ProducerScheduledEvent extends AbstractTableEvent {
    private final CountDownLatch startLatch;

    public ProducerScheduledEvent(Table table, Path path, CountDownLatch startLatch) {
        super(table, path);
        this.startLatch = startLatch;
    }

    public CountDownLatch getStartLatch() {
        return startLatch;
    }
}
