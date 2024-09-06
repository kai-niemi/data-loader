package io.roach.volt.csv.file;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.pubsub.EmptyTopic;
import io.roach.volt.pubsub.Message;
import io.roach.volt.pubsub.Topic;

public class UpstreamChunkProducer extends AsyncChunkProducer {
    @Override
    protected void doInitialize() {
        table.filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(this::subscribeTo);
    }

    @Override
    public void produceChunks(ChunkConsumer<String, Object> consumer) throws Exception {
        Topic<Map<String, Object>> topic = publisher.getTopic(table.getName());

        if (!topic.hasMessageListeners()) {
            topic = new EmptyTopic<>();
        }

        for (int i = 0; i < table.getFinalCount(); i++) {
            Map<String, Map<String, Object>> refMap = new HashMap<>();
            Map<String, Object> orderedTuples = new LinkedHashMap<>();

            for (Column col : table.getColumns()) {
                Ref ref = col.getRef();
                Object v = ref != null ? consumeFrom(refMap, ref) : columnGenerators.get(col).nextValue();
                orderedTuples.put(col.getName(), v);
            }

            topic.publish(Message.of(orderedTuples));

            Map<String, Object> copy = filterIncludedValues(orderedTuples);

            currentRow.incrementAndGet();

            if (!consumer.consumeChunk(copy, table.getFinalCount())) {
                break;
            }
        }

        topic.publish(Message.poisonPill());
    }
}
