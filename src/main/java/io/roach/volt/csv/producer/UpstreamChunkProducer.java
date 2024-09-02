package io.roach.volt.csv.producer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.util.pubsub.Message;
import io.roach.volt.util.pubsub.MessageListener;
import io.roach.volt.util.pubsub.Topic;

public class UpstreamChunkProducer extends AsyncChunkProducer {
    @Override
    protected void doInitialize() {
        Set<String> refs = new HashSet<>();

        table.filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(ref -> {
                    logger.debug("Upstream producer '%s' subscribing to random items of '%s'"
                            .formatted(table.getName(), ref.getColumn()));

                    if (refs.add(ref.getName())) {
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

    @Override
    public void produceChunks(ChunkConsumer<String, Object> consumer) throws Exception {
        Topic<Map<String, Object>> topic = publisher.getTopic(table.getName());
        if (!topic.hasMessageListeners()) {
            topic = new Topic.Empty<>();
        }

        for (int i = 0; i < table.getFinalCount(); i++) {
            Map<String, Object> orderedValues = new LinkedHashMap<>();
            Map<String, Map<String, Object>> observedMap = new HashMap<>();

            for (Column col : table.filterColumns(column -> true)) {
                Object v;
                Ref ref = col.getRef();
                if (ref != null) {
                    Map<String, Object> refValues =
                            observedMap.computeIfAbsent(ref.getName(), fifoQueue::selectRandom);
                    v = refValues.get(ref.getColumn());
                } else {
                    v = columnGenerators.get(col).nextValue();
                }

                orderedValues.put(col.getName(), v);
            }

            topic.publish(orderedValues);

            Map<String, Object> copy = filterIncludes(orderedValues);

            if (!consumer.consumeChunk(copy, table.getFinalCount())) {
                break;
            }
        }

        topic.publish(Map.of()); // poison pill
    }
}
