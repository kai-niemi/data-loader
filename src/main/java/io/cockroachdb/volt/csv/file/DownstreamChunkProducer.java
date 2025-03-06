package io.cockroachdb.volt.csv.file;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import io.cockroachdb.volt.csv.ConfigurationException;
import io.cockroachdb.volt.csv.model.Column;
import io.cockroachdb.volt.csv.model.Each;
import io.cockroachdb.volt.csv.model.Ref;
import io.cockroachdb.volt.csv.model.Table;
import io.cockroachdb.volt.pubsub.EmptyTopic;
import io.cockroachdb.volt.pubsub.Message;
import io.cockroachdb.volt.pubsub.Topic;

/**
 * A downstream producer consumes key-value pairs from an upstream producer
 * referenced by a singleton ref column. The consumption uses the bounded
 * blocking queue and each consumed pair can be multiplied, determined by
 * the ref configuration.
 */
public class DownstreamChunkProducer extends AsyncChunkProducer {
    @Override
    protected void doInitialize() {
        final Each each = upstreamEachSingleton();

        subscribeTo(each);

        table.filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(this::subscribeTo);
    }

    private Each upstreamEachSingleton() {
        return table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .findFirst()
                .orElseThrow(() -> new ConfigurationException(
                        "Expected singleton each for downstream producer", table));
    }

    @Override
    public void produceChunks(ChunkConsumer<String, Object> consumer) throws Exception {
        final Each each = upstreamEachSingleton();

        Topic<Map<String, Object>> topic = publisher.getTopic(table.getName());
        if (!topic.hasMessageListeners()) {
            topic = new EmptyTopic<>();
        }

        final int rowEstimate = -1;

        // Wait for upstream values or poison pill to cancel
        Map<String, Object> upstreamTuples = boundedFifoQueue.take(each.getName());
        while (!upstreamTuples.isEmpty()) {
            // Repeat if needed
            for (int n = 0; n < each.getMultiplier(); n++) {
                final Map<String, Map<String, Object>> refMap = new HashMap<>();
                final Map<String, Object> orderedTuples = new LinkedHashMap<>();

                for (Column column : table.getColumns()) {
                    Object v;
                    if (each.equals(column.getEach())) {
                        v = upstreamTuples.get(each.getColumn());
                        if (Objects.isNull(v)) {
                            throw new ConfigurationException("Column each ref not found: %s"
                                    .formatted(each), table);
                        }
                    } else {
                        Ref ref = column.getRef();
                        if (ref != null) {
                            if (ref.getName().equals(each.getName())) {
                                v = upstreamTuples.get(ref.getColumn());
                                if (Objects.isNull(v)) {
                                    throw new ConfigurationException("Column ref not found: %s"
                                            .formatted(ref), table);
                                }
                            } else {
                                v = consumeFrom(refMap, ref);
                            }
                        } else {
                            v = columnGenerators.get(column).nextValue();
                        }
                    }
                    orderedTuples.put(column.getName(), v);
                }

                topic.publish(Message.of(orderedTuples));

                Map<String, Object> copy = filterIncludedValues(orderedTuples);

                currentRow.incrementAndGet();

                if (!consumer.consumeChunk(copy, rowEstimate)) {
                    break;
                }
            }

            upstreamTuples = boundedFifoQueue.take(each.getName());
        }

        topic.publish(Message.poisonPill());
    }
}

