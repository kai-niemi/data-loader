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
    public void produceChunks(ChunkConsumer<String, Object> consumer) throws Exception {
        final Each each = subscribeToUpstreamTopic();

        final Topic<Map<String, Object>> topic = publisher.getTopic(table.getName());
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

                topic.publishAsync(orderedMap);

                Map<String, Object> copy = filterIncludes(orderedMap);

                if (!consumer.consumeChunk(copy, rowEstimate)) {
                    break;
                }
            }

            upstreamValues = fifoQueue.take(each.getName());
        }

        topic.publishAsync(Map.of()); // poison pill
    }

    private Each subscribeToUpstreamTopic() {
        Each each = table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .findFirst()
                .orElseThrow();

        logger.debug("Downstream producer '%s' subscribing to each item of '%s'"
                .formatted(table.getName(), each.getName()));

        publisher.<Map<String, Object>>getTopic(each.getName())
                .addMessageListener(new MessageListener<>() {
//                    @Override
//                    public String getName() {
//                        return "Downstream each for table '%s' column '%s'"
//                                .formatted(table.getName(), each.getColumn());
//                    }

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
                    logger.debug("Downstream producer '%s' subscribing to random items of '%s'"
                            .formatted(table.getName(), ref.getName()));

                    publisher.<Map<String, Object>>getTopic(ref.getName())
                            .addMessageListener(new MessageListener<>() {
//                                @Override
//                                public String getName() {
//                                    return "Downstream ref for table '%s' column '%s'"
//                                            .formatted(table.getName(), ref.getColumn());
//                                }

                                @Override
                                public void onMessage(Message<Map<String, Object>> message) {
                                    fifoQueue.offer(message.getTopic(), message.getPayload());
                                }
                            });
                });

        return each;
    }
}

