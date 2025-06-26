package io.cockroachdb.dl.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import io.cockroachdb.dl.core.generator.ValueGenerator;
import io.cockroachdb.dl.core.generator.ValueGenerators;
import io.cockroachdb.dl.core.model.Column;
import io.cockroachdb.dl.core.model.Each;
import io.cockroachdb.dl.core.model.Ref;
import io.cockroachdb.dl.core.model.Table;
import io.cockroachdb.dl.expression.ExpressionRegistry;
import io.cockroachdb.dl.expression.ExpressionRegistryBuilder;
import io.cockroachdb.dl.expression.FunctionDef;
import io.cockroachdb.dl.pubsub.Publisher;
import io.cockroachdb.dl.util.concurrent.BlockingFifoQueue;
import io.cockroachdb.dl.util.concurrent.CircularFifoQueue;
import io.cockroachdb.dl.util.concurrent.FifoQueue;

/**
 * Base class for async chuck producers using topics and FIFO queues
 * for message passing to avoid building state in table relations.
 */
public abstract class AsyncChunkProducer implements ChunkProducer<String, Object>, AsyncProducer {
    protected static final Predicate<Column> COLUMN_INCLUDE_PREDICATE
            = column -> (column.isHidden() == null || !column.isHidden());

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final Map<Column, ValueGenerator<?>> columnGenerators = new LinkedHashMap<>();

    protected final FifoQueue<String, Object> boundedFifoQueue
            = new BlockingFifoQueue<>(8192);

    protected final FifoQueue<String, Object> circularFifoQueue
            = new CircularFifoQueue<>(8192);

    protected DataSource dataSource;

    protected Publisher publisher;

    protected Table table;

    protected final AtomicInteger currentRow = new AtomicInteger(1);

    @Override
    public Supplier<Integer> currentRow() {
        return currentRow::get;
    }

    /**
     * Filter values in a map based on the column include predicate.
     *
     * @param map the source value map
     * @return the filtered target map
     */
    protected Map<String, Object> filterIncludedValues(Map<String, Object> map) {
        Map<String, Object> copy = new LinkedHashMap<>();

        table.getColumns()
                .stream()
                .filter(COLUMN_INCLUDE_PREDICATE)
                .map(Column::getName)
                .forEach(column -> copy.put(column, map.get(column)));

        return copy;
    }

    /**
     * Add a subscription for the table referenced from a given each column ref.
     * Each received event will be put into the bounded blocking queue for consumption.
     *
     * @param each the each ref
     */
    protected void subscribeTo(Each each) {
        publisher.<Map<String, Object>>getTopic(each.getName())
                .addMessageListener(message -> {
                    try {
                        if (message.isPoisonPill()) {
                            boundedFifoQueue.put(each.getName(), Map.of());
                        } else {
                            boundedFifoQueue.put(each.getName(), message.getPayload());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new ProducerFailedException("Interrupted put() for key " + each.getName(), e);
                    }
                });
    }

    /**
     * Add a subscription for the table referenced from a given column ref.
     * Each received event will be put into the circular blocking queue for consumption.
     *
     * @param ref the column ref
     */
    protected void subscribeTo(Ref ref) {
        publisher.<Map<String, Object>>getTopic(ref.getName())
                .addMessageListener(message -> {
                    if (!message.isPoisonPill()) {
                        try {
                            circularFifoQueue.put(ref.getName(), message.getPayload());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new ProducerFailedException("Interrupted put() for key " + ref.getName(),
                                    e);
                        }
                    }
                });
    }

    /**
     * Consume values from a ref map, in turn feeding from the circular queue.
     *
     * @param refMap the ref map, producer scoped
     * @param ref    the column ref
     * @return the ref value, must not be null
     */
    protected Object consumeFrom(Map<String, Map<String, Object>> refMap, Ref ref) {
        Map<String, Object> values = refMap.computeIfAbsent(ref.getName(), key -> {
            try {
                return circularFifoQueue.take(key);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProducerFailedException("Interrupted take() for key " + key, e);
            }
        });

        Object v = values.get(ref.getColumn());
        if (Objects.isNull(v)) {
            throw new ConfigurationException("Column ref not found: %s"
                    .formatted(ref), table);
        }

        return v;
    }

    /**
     * Perform initializations needed for all async chunk producers.
     */
    @Override
    public final void initialize(DataSource dataSource, Publisher publisher, Table table) {
        Assert.notNull(dataSource, "dataSource is null");
        Assert.notNull(publisher, "publisher is null");
        Assert.notNull(table, "table is null");

        this.dataSource = dataSource;
        this.publisher = publisher;
        this.table = table;

        ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);

        List.of(FunctionDef.builder()
                        .withCategory("other")
                        .withId("rowNumber")
                        .withDescription("Returns the current row number.")
                        .withReturnValue(Integer.class)
                        .withFunction(args -> currentRow().get())
                        .build())
                .forEach(registry::addFunction);

        // Get generator for all non-ref columns
        table.filterColumns(Table.WITH_REF.or(Table.WITH_EACH).negate())
                .forEach(column -> columnGenerators.put(column,
                        ValueGenerators.createValueGenerator(column, dataSource, registry)));

        doInitialize();
    }

    /**
     * Perform custom initializations needed for subclassed producers.
     */
    protected void doInitialize() {
    }
}