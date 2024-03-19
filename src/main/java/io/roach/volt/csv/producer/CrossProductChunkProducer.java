package io.roach.volt.csv.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.springframework.util.Assert;

import io.roach.volt.csv.generator.ColumnGenerator;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Each;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.util.Cartesian;
import io.roach.volt.util.pubsub.Publisher;
import io.roach.volt.util.pubsub.Topic;

public class CrossProductChunkProducer extends AbstractChunkProducer<String, Object> {
    private static final int WARN_THRESHOLD = 10_000_000;

    public CrossProductChunkProducer(Table table,
                                     Map<Column, ColumnGenerator<?>> columnGenerators,
                                     int queueCapacity) {
        super(table, columnGenerators, queueCapacity);
    }

    @Override
    protected void initialize(Publisher publisher) {
        Set<String> topics = new HashSet<>();

        getTable().filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(each -> {
                    topics.add(each.getName());

                    getLogger().debug("Cross-product (cartesian) producer '%s' subscribing to each item of '%s'"
                            .formatted(getTable().getName(), each.getName()));

                    publisher.<Map<String, Object>>getTopic(each.getName())
                            .addMessageListener(message -> {
                                getFifoQueue().put(message.getTopic(), message.getPayload());
                            });
                });

        getTable().filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(ref -> {
                    if (!topics.contains(ref.getName())) {
                        topics.add(ref.getName());

                        getLogger().debug("Cross-product (cartesian) producer '%s' subscribing to random items of '%s'"
                                .formatted(getTable().getName(), ref.getName()));

                        publisher.<Map<String, Object>>getTopic(ref.getName())
                                .addMessageListener(message -> {
                                    getFifoQueue().offer(message.getTopic(), message.getPayload());
                                });
                    }
                });
    }

    private void drainQueue(String topic, Consumer<Map<String, Object>> consumer) {
        getLogger().debug("Draining queue %s".formatted(topic));
        int c = 0;
        Map<String, Object> values = getFifoQueue().take(topic);
        while (!values.isEmpty()) {
            c++;
            consumer.accept(values);
            values = getFifoQueue().take(topic);
        }
        getLogger().debug("Drained queue %s with %d rows"
                .formatted(topic, c));
    }

    @Override
    protected void doProduce(Publisher publisher,
                             ChunkConsumer<String, Object> consumer) {
        Map<String, List<Map<String, Object>>> columnValueMap = new ConcurrentHashMap<>();
        List<List<Map<String, Object>>> columnSets = new ArrayList<>();
        Map<String, Integer> columnIndex = new HashMap<>();
        AtomicInteger idx = new AtomicInteger();

        Topic<Map<String, Object>> topic = publisher.getTopic(getTable().getName());

        getLogger().debug("Starting to drain queues to build cartesian product");

        getTable().filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(each -> {
                    if (!columnValueMap.containsKey(each.getName())) {
                        List<Map<String, Object>> rows = new ArrayList<>();
                        drainQueue(each.getName(), rows::add);
                        columnValueMap.put(each.getName(), rows);
                    }
                });

        getTable().filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(each -> {
                    List<Map<String, Object>> allValues = columnValueMap.get(each.getName());
                    columnSets.add(allValues);
                    columnIndex.put(each.getName(), idx.getAndIncrement());
                });

        // Calc total items in cartesian product (N*N*N..)
        final long rowEstimate = columnSets.stream()
                .mapToLong(List::size)
                .reduce(1, Math::multiplyExact);

        if (rowEstimate > WARN_THRESHOLD) {
            getLogger().warn("Potentially very large cartesian product for '%s' with %,d rows"
                    .formatted(getTable().getName(), rowEstimate));
        }

        getLogger().debug("Drained all queues for cartesian product of %,d rows - starting to stream"
                .formatted(rowEstimate));

        // Create cartesian product from columns sets
        Stream<List<Map<String, Object>>> cartesianProduct = Cartesian.cartesianProductStream(columnSets);

        // Short-circuit
        AtomicBoolean cancel = new AtomicBoolean();

        // Now we have the whole cartesian product that can be streamed out
        cartesianProduct
                .takeWhile(objects -> !cancel.get())
                .forEach(productMap -> {
                    Map<String, Object> orderedValues = new LinkedHashMap<>();

                    getTable().filterColumns(column -> true)
                            .forEach(column -> {
                                Object v;

                                Each each = column.getEach();
                                if (each != null) {
                                    Assert.isTrue(columnIndex.containsKey(each.getName()),
                                            "Expected each: " + each.getName());
                                    v = productMap.get(columnIndex.get(each.getName())).get(each.getColumn());
                                } else {
                                    Ref ref = column.getRef();
                                    if (ref != null) {
                                        if (columnIndex.containsKey(ref.getName())) {
                                            v = productMap.get(columnIndex.get(ref.getName())).get(ref.getColumn());
                                        } else {
                                            v = getFifoQueue().selectRandom(ref.getName());
                                        }
                                    } else {
                                        v = getColumnGenerators().get(column).nextValue();
                                    }
                                }
                                orderedValues.put(column.getName(), v);
                            });

                    topic.publishAsync(orderedValues);

                    if (!consumer.consume(orderedValues, rowEstimate)) {
                        cancel.set(true);
                    }
                });

        topic.publishAsync(Map.of()); // poison pill
    }
}
