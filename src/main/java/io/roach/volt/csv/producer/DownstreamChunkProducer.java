package io.roach.volt.csv.producer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Each;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.util.pubsub.Message;
import io.roach.volt.util.pubsub.MessageListener;
import io.roach.volt.util.pubsub.Topic;

public class DownstreamChunkProducer extends AsyncChunkProducer {
    @Override
    protected void doInitialize() {
        Each each = findUpstreamEach();

        logger.debug("Downstream producer '%s' subscribing to each item of '%s'"
                .formatted(table.getName(), each.getName()));

        publisher.<Map<String, Object>>getTopic(each.getName())
                .addMessageListener(new MessageListener<>() {
                    @Override
                    public void onMessage(Message<Map<String, Object>> message) {
                        if (message.getPayload().isEmpty()) {
                            logger.debug("Downstream producer '%s' received poison pill for '%s'"
                                    .formatted(table.getName(), each.getName()));
                        }
                        fifoQueue.put(message.getTopic(), message.getPayload());
                    }
                });

        table.filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(ref -> {
                    publisher.<Map<String, Object>>getTopic(ref.getName())
                            .addMessageListener(new MessageListener<>() {
                                @Override
                                public void onMessage(Message<Map<String, Object>> message) {
                                    fifoQueue.offer(message.getTopic(), message.getPayload());
                                }
                            });
                });
    }

    private Each findUpstreamEach() {
        return table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .findFirst()
                .orElseThrow();
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
                                Map<String, Object> values =
                                        refMap.computeIfAbsent(ref.getName(), fifoQueue::selectRandom);
                                v = values.get(ref.getColumn());
                            }
                        } else {
                            v = columnGenerators.get(col).nextValue();
                        }
                    }
                    orderedMap.put(col.getName(), v);
                }

                topic.publish(orderedMap);

                Map<String, Object> copy = filterIncludes(orderedMap);

                if (!consumer.consumeChunk(copy, rowEstimate)) {
                    break;
                }
            }

            upstreamValues = fifoQueue.take(each.getName());
        }

        topic.publish(Map.of()); // poison pill
    }
}

