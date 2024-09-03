package io.roach.volt.csv.producer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.pubsub.Message;
import io.roach.volt.pubsub.Topic;

public class UpstreamChunkProducer extends AsyncChunkProducer {
    @Override
    protected void doInitialize() {
        table.filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(ref -> {
                    publisher.<Map<String, Object>>getTopic(ref.getName())
                            .addMessageListener(message -> {
                                if (!message.isPoisonPill()) {
                                    fifoQueue.offer(ref.getName(), message.getPayload());
                                }
                            });
                });
    }

    @Override
    public void produceChunks(ChunkConsumer<String, Object> consumer) throws Exception {
        Topic<Map<String, Object>> topic = publisher.getTopic(table.getName());
        if (!topic.hasMessageListeners()) {
            topic = new Topic.Empty<>();
        }

        outer:
        for (int i = 0; i < table.getFinalCount(); i++) {
            Map<String, Object> orderedValues = new LinkedHashMap<>();
            Map<String, Map<String, Object>> observedMap = new HashMap<>();

            for (Column col : table.filterColumns(column -> true)) {
                Object v;
                Ref ref = col.getRef();
                if (ref != null) {
                    Map<String, Object> refValues =
                            observedMap.computeIfAbsent(ref.getName(), fifoQueue::peekRandom);
                    if (refValues.isEmpty()) {
                        logger.info("Poison pill for ref column '%s' - breaking".formatted(ref.getName()));
                        break outer;
                    }
                    v = refValues.get(ref.getColumn());
                } else {
                    v = columnGenerators.get(col).nextValue();
                }


                orderedValues.put(col.getName(), v);
            }

            topic.publish(Message.of(orderedValues));

            Map<String, Object> copy = filterIncludes(orderedValues);

            if (!consumer.consumeChunk(copy, table.getFinalCount())) {
                break;
            }
        }

        topic.publish(Message.poisonPill()); // poison pill
    }
}
