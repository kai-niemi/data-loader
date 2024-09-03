package io.roach.volt.csv.producer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Each;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.pubsub.Message;
import io.roach.volt.pubsub.Topic;

public class DownstreamChunkProducer extends AsyncChunkProducer {
    private Each findUpstreamEach() {
        return table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .findFirst()
                .orElseThrow();
    }

    @Override
    protected void doInitialize() {
        Each each = findUpstreamEach();

        logger.debug("Downstream producer '%s' subscribing to each item of '%s'"
                .formatted(table.getName(), each.getName()));

        publisher.<Map<String, Object>>getTopic(each.getName())
                .addMessageListener(message -> {
                    if (message.isPoisonPill()) {
                        fifoQueue.put(each.getName(), Map.of());
                    } else {
                        fifoQueue.put(each.getName(), message.getPayload());
                    }
                });

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
        Each each = findUpstreamEach();

        Topic<Map<String, Object>> topic = publisher.getTopic(table.getName());
        if (!topic.hasMessageListeners()) {
            topic = new Topic.Empty<>();
        }

        final int rowEstimate = -1;

        // Wait for upstream values or poison pill to cancel (empty collection)
        Map<String, Object> upstreamValues = fifoQueue.take(each.getName());
        loop:
        while (!upstreamValues.isEmpty()) {
            // Repeat if needed
            for (int n = 0; n < each.getMultiplier(); n++) {
                final Map<String, Map<String, Object>> refMap = new HashMap<>();
                final Map<String, Object> orderedMap = new LinkedHashMap<>();

                for (Column col : table.filterColumns(column -> true)) {
                    Object v;
                    if (col.getEach() != null) {
                        v = upstreamValues.get(each.getColumn());
                    } else {
                        Ref ref = col.getRef();
                        if (ref != null) {
                            if (ref.getName().equals(each.getName())) {
                                v = upstreamValues.get(ref.getColumn());
                            } else {
                                Map<String, Object> values
                                        = refMap.computeIfAbsent(ref.getName(), fifoQueue::peekRandom);
                                v = values.get(ref.getColumn());
                                if (values.isEmpty()) {
                                    logger.info("Poison pill for ref column '%s' - breaking".formatted(ref.getName()));
                                    break loop;
                                }
                            }
                        } else {
                            v = columnGenerators.get(col).nextValue();
                        }
                    }
                    orderedMap.put(col.getName(), v);
                }

                topic.publish(Message.of(orderedMap));

                Map<String, Object> copy = filterIncludes(orderedMap);

                if (!consumer.consumeChunk(copy, rowEstimate)) {
                    break;
                }
            }

            upstreamValues = fifoQueue.take(each.getName());
        }

        topic.publish(Message.poisonPill()); // poison pill
    }
}

