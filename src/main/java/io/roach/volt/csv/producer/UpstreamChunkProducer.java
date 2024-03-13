package io.roach.volt.csv.producer;

import io.roach.volt.csv.generator.ColumnGenerator;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.util.pubsub.Publisher;
import io.roach.volt.util.pubsub.Topic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class UpstreamChunkProducer extends AbstractChunkProducer<String, Object> {
    public UpstreamChunkProducer(Table table,
                                 Map<Column, ColumnGenerator<?>> columnGenerators,
                                 int queueCapacity) {
        super(table, columnGenerators, queueCapacity);
    }

    @Override
    protected void initialize(Publisher publisher) {
        Set<String> upstreamRefs = new HashSet<>();

        getTable().filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(ref -> upstreamRefs.add(ref.getName()));

        upstreamRefs.forEach(ref -> {
            getLogger().debug("Upstream producer '%s' subscribing to random items of '%s'"
                    .formatted(getTable().getName(), ref));

            publisher.<Map<String, Object>>getTopic(ref)
                    .addMessageListener(message ->
                            getFifoQueue().offer(message.getTopic(), message.getPayload()));
        });
    }

    @Override
    protected void doProduce(Publisher publisher,
                             ChunkConsumer<String, Object> consumer) {
        Topic<Map<String, Object>> topic = publisher.getTopic(getTable().getName());

        for (int i = 0; i < getTable().getFinalCount(); i++) {
            Map<String, Map<String, Object>> observedMap = new HashMap<>();
            Map<String, Object> orderedMap = new LinkedHashMap<>();

            for (Column col : getTable().filterColumns(column -> true)) {
                Object v;
                Ref ref = col.getRef();
                if (ref != null) {
                    Map<String, Object> refValues =
                            observedMap.computeIfAbsent(ref.getName(), getFifoQueue()::selectRandom);
                    v = refValues.get(ref.getColumn());
                } else {
                    v = getColumnGenerators().get(col).nextValue();
                }

                orderedMap.put(col.getName(), v);
            }

            topic.publishAsync(orderedMap);

            if (!consumer.consume(orderedMap, getTable().getFinalCount())) {
                break;
            }
        }

        topic.publishAsync(Map.of()); // poison pill
    }
}
