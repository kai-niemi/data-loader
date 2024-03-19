package io.roach.volt.csv.producer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.roach.volt.csv.generator.ColumnGenerator;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Table;
import io.roach.volt.util.concurrent.CircularFifoQueue;
import io.roach.volt.util.concurrent.FifoQueue;
import io.roach.volt.util.pubsub.Publisher;

public abstract class AbstractChunkProducer<K, V> implements ChunkProducer<K, V> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Table table;

    private final Map<Column, ColumnGenerator<?>> columnGenerators;

    private final FifoQueue<K, V> fifoQueue;

    protected AbstractChunkProducer(Table table,
                                    Map<Column, ColumnGenerator<?>> columnGenerators,
                                    int queueCapacity) {
        this.table = table;
        this.columnGenerators = columnGenerators;
        this.fifoQueue = new CircularFifoQueue<>(queueCapacity);
    }

    protected Logger getLogger() {
        return logger;
    }

    protected Table getTable() {
        return table;
    }

    protected Map<Column, ColumnGenerator<?>> getColumnGenerators() {
        return columnGenerators;
    }

    protected FifoQueue<K, V> getFifoQueue() {
        return fifoQueue;
    }

    @Override
    public final void produce(Publisher publisher,
                              ChunkConsumer<K, V> consumer) {
        getLogger().debug("%s initializing for table '%s'"
                .formatted(getClass().getSimpleName(), getTable().getName()));

        initialize(publisher);

        try {
            getLogger().debug("%s starting for table '%s'"
                    .formatted(getClass().getSimpleName(), getTable().getName()));

            doProduce(publisher, consumer);

            getLogger().debug("%s finished for table '%s'"
                    .formatted(getClass().getSimpleName(), getTable().getName()));

        } finally {
            cleanup(publisher);
        }
    }

    protected void initialize(Publisher publisher) {
    }

    protected abstract void doProduce(Publisher publisher,
                                      ChunkConsumer<K, V> consumer);

    protected void cleanup(Publisher publisher) {
    }
}