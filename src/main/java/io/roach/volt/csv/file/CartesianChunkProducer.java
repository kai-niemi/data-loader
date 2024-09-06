package io.roach.volt.csv.file;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.Assert;

import io.roach.volt.csv.ConfigurationException;
import io.roach.volt.csv.ProducerFailedException;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Each;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.pubsub.EmptyTopic;
import io.roach.volt.pubsub.Message;
import io.roach.volt.pubsub.Topic;
import io.roach.volt.util.Cartesian;

/**
 * A cartesian (cross-product) producer is mapped to tables with more
 * than one each ref column typical for many-to-many relations.
 * It consumes, aggregates and computes a cartesian product of all
 * ref column permutations. This can be time-consuming and has combinatorial
 * complexity with high memory consumption, thus use with caution.
 */
public class CartesianChunkProducer extends AsyncChunkProducer {
    private static final int WARN_THRESHOLD = 10_000_000;

    @Override
    protected void doInitialize() {
        table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(this::subscribeTo);

        table.filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(this::subscribeTo);
    }

    private List<List<Map<String, Object>>> drainUpStreamTopics() {
        Map<String, List<Map<String, Object>>> columnValueMap = new LinkedHashMap<>();

        table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(each -> {
                    if (!columnValueMap.containsKey(each.getName())) {
                        List<Map<String, Object>> rows = new LinkedList<>();

                        try {
                            Map<String, Object> values = boundedFifoQueue.take(each.getName());
                            while (!values.isEmpty()) {
                                rows.add(values);
                                values = boundedFifoQueue.take(each.getName());
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new ProducerFailedException("Interrupted take() for key " + each.getName(), e);
                        }

                        columnValueMap.put(each.getName(), rows);
                    }
                });

        return columnValueMap
                .keySet()
                .stream()
                .map(columnValueMap::get)
                .collect(Collectors.toList());
    }

    private Map<String, Integer> resolveColumnIndexes() {
        Map<String, Integer> columnIndexes = new HashMap<>();
        AtomicInteger idx = new AtomicInteger();

        table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(each -> columnIndexes.put(each.getName(), idx.getAndIncrement()));

        return columnIndexes;
    }

    @Override
    public void produceChunks(ChunkConsumer<String, Object> consumer) throws Exception {
        List<List<Map<String, Object>>> columnSets = drainUpStreamTopics();

        long rowEstimate = columnSets.stream()
                .mapToLong(List::size)
                .reduce(1, Math::multiplyExact);

        if (rowEstimate > WARN_THRESHOLD) {
            logger.warn("Potentially large cartesian product for '%s' with %,d rows"
                    .formatted(table.getName(), rowEstimate));
        } else {
            logger.debug("Drained queues for '%s' with %,d rows - starting to stream"
                    .formatted(table.getName(), rowEstimate));
        }

        Map<String, Integer> columnIndexes = resolveColumnIndexes();

        Topic<Map<String, Object>> topic = publisher.getTopic(table.getName());
        if (!topic.hasMessageListeners()) {
            topic = new EmptyTopic<>();
        }

        // Create cartesian product from columns sets
        Stream<List<Map<String, Object>>> cartesianProduct = Cartesian.cartesianProductStream(columnSets);

        // Short-circuit
        AtomicBoolean cancel = new AtomicBoolean();

        // Now we have the whole cartesian product that can be streamed out
        for (List<Map<String, Object>> productMap : cartesianProduct
                .takeWhile(objects -> !cancel.get())
                .toList()) {

            Map<String, Object> orderedTuples = new LinkedHashMap<>();

            for (Column c : table.getColumns()) {
                Object v;

                Each each = c.getEach();
                if (each != null) {
                    Assert.isTrue(columnIndexes.containsKey(each.getName()),
                            "Expected each: " + each.getName());
                    v = productMap.get(columnIndexes.get(each.getName())).get(each.getColumn());
                } else {
                    Ref ref = c.getRef();
                    if (ref != null) {
                        if (columnIndexes.containsKey(ref.getName())) {
                            v = productMap.get(columnIndexes.get(ref.getName())).get(ref.getColumn());
                            if (Objects.isNull(v)) {
                                throw new ConfigurationException("Column ref not found: %s"
                                        .formatted(ref), table);
                            }
                        } else {
                            v = circularFifoQueue.take(ref.getName());
                        }
                    } else {
                        v = columnGenerators.get(c).nextValue();
                    }
                }
                orderedTuples.put(c.getName(), v);
            }

            topic.publish(Message.of(orderedTuples));

            Map<String, Object> copy = filterIncludedValues(orderedTuples);

            currentRow.incrementAndGet();

            if (!consumer.consumeChunk(copy, rowEstimate)) {
                cancel.set(true);
            }
        }

        topic.publish(Message.poisonPill());
    }
}
