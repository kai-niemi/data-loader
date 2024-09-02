package io.roach.volt.csv.producer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.Assert;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Each;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.util.Cartesian;
import io.roach.volt.util.pubsub.Message;
import io.roach.volt.util.pubsub.MessageListener;
import io.roach.volt.util.pubsub.Topic;

public class CartesianChunkProducer extends AsyncChunkProducer {
    private static final int WARN_THRESHOLD = 10_000_000;

    @Override
    protected void doInitialize() {
        Set<String> eachSet = new HashSet<>();

        table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(each -> {
                    if (eachSet.add(each.getName())) {
                        publisher.<Map<String, Object>>getTopic(each.getName())
                                .addMessageListener(new MessageListener<>() {
                                    @Override
                                    public void onMessage(Message<Map<String, Object>> message) {
                                        fifoQueue.put(message.getTopic(), message.getPayload());
                                    }
                                });
                    }
                });

        Set<String> refSet = new HashSet<>();

        table.filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(ref -> {
                    if (refSet.add(ref.getName())) {
                        publisher.<Map<String, Object>>getTopic(ref.getName())
                                .addMessageListener(new MessageListener<>() {
                                    @Override
                                    public void onMessage(Message<Map<String, Object>> message) {
                                        fifoQueue.offer(message.getTopic(), message.getPayload());
                                    }
                                });
                    }
                });
    }

    private List<List<Map<String, Object>>> drainUpStreamTopics() {
        Map<String, List<Map<String, Object>>> columnValueMap = new LinkedHashMap<>();

        table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(each -> {
                    if (!columnValueMap.containsKey(each.getName())) {
                        List<Map<String, Object>> rows = new LinkedList<>();

                        Map<String, Object> values = fifoQueue.take(each.getName());
                        while (!values.isEmpty()) {
                            rows.add(values);
                            values = fifoQueue.take(each.getName());
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
            logger.warn("Potentially very large cartesian product for '%s' with %,d rows"
                    .formatted(table.getName(), rowEstimate));
        } else {
            logger.debug("Drained queues for '%s' with %,d rows - starting to stream"
                    .formatted(table.getName(), rowEstimate));
        }

        Map<String, Integer> columnIndexes = resolveColumnIndexes();

        Topic<Map<String, Object>> topic = publisher.getTopic(table.getName());
        if (!topic.hasMessageListeners()) {
            topic = new Topic.Empty<>();
        }

        // Create cartesian product from columns sets
        Stream<List<Map<String, Object>>> cartesianProduct = Cartesian.cartesianProductStream(columnSets);

        // Short-circuit
        AtomicBoolean cancel = new AtomicBoolean();

        // Now we have the whole cartesian product that can be streamed out
        for (List<Map<String, Object>> productMap : cartesianProduct
                .takeWhile(objects -> !cancel.get())
                .toList()) {

            Map<String, Object> orderedValues = new LinkedHashMap<>();

            table.filterColumns(column -> true)
                    .forEach(column -> {
                        Object v;

                        Each each = column.getEach();
                        if (each != null) {
                            Assert.isTrue(columnIndexes.containsKey(each.getName()),
                                    "Expected each: " + each.getName());
                            v = productMap.get(columnIndexes.get(each.getName())).get(each.getColumn());
                        } else {
                            Ref ref = column.getRef();
                            if (ref != null) {
                                if (columnIndexes.containsKey(ref.getName())) {
                                    v = productMap.get(columnIndexes.get(ref.getName())).get(ref.getColumn());
                                } else {
                                    v = fifoQueue.selectRandom(ref.getName());
                                }
                            } else {
                                v = columnGenerators.get(column).nextValue();
                            }
                        }
                        orderedValues.put(column.getName(), v);
                    });

            topic.publish(orderedValues);

            Map<String, Object> copy = filterIncludes(orderedValues);

            if (!consumer.consumeChunk(copy, rowEstimate)) {
                cancel.set(true);
            }
        }

        topic.publish(Map.of()); // poison pill
    }
}
