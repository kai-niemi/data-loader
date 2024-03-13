package io.roach.volt.csv.producer;

import io.roach.volt.csv.generator.ColumnGenerator;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Each;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Table;
import io.roach.volt.util.concurrent.ConcurrencyUtils;
import io.roach.volt.util.pubsub.Publisher;
import io.roach.volt.util.pubsub.Topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class DownstreamChunkProducer extends AbstractChunkProducer<String, Object> {
    private final Set<Each> upstreamEach = new HashSet<>();

    public DownstreamChunkProducer(Table table,
                                   Map<Column, ColumnGenerator<?>> columnGenerators,
                                   int queueCapacity) {
        super(table, columnGenerators, queueCapacity);
    }

    @Override
    protected void initialize(Publisher publisher) {
        getTable().filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach)
                .forEach(upstreamEach::add);

        upstreamEach.forEach(each -> {
            getLogger().debug("Downstream producer '%s' subscribing to each item of '%s'"
                    .formatted(getTable().getName(), each.getName()));

            publisher.<Map<String, Object>>getTopic(each.getName())
                    .addMessageListener(message -> {
                        if (message.getPayload().isEmpty()) {
                            getLogger().debug("Downstream producer '%s' received poison pill for '%s'"
                                    .formatted(getTable().getName(), each.getName()));
                        }
                        getFifoQueue().put(message.getTopic(), message.getPayload());
                    });
        });

        getTable().filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef)
                .forEach(ref -> {
                    long c = upstreamEach.stream()
                            .map(Each::getName)
                            .filter(name -> name.equals(ref.getName()))
                            .count();
                    if (c == 0) {
                        getLogger().debug("Downstream producer '%s' subscribing to random items of '%s'"
                                .formatted(getTable().getName(), ref.getName()));

                        publisher.<Map<String, Object>>getTopic(ref.getName())
                                .addMessageListener(message -> {
                                    getFifoQueue().offer(message.getTopic(), message.getPayload());
                                });
                    }
                });
    }

    @Override
    protected void doProduce(Publisher publisher,
                             ChunkConsumer<String, Object> consumer) {
        final Topic<Map<String, Object>> topic = publisher.getTopic(getTable().getName());

        final Each each = upstreamEach.stream().iterator().next();

        final int rowEstimate = -1;

        final Map<String, Object> upstreamValues = getFifoQueue().take(each.getName());

        while (!upstreamValues.isEmpty()) {
            for (int n = 0; n < each.getMultiplier(); n++) {
                final Map<String, Map<String, Object>> refMap = new HashMap<>();
                final Map<String, Object> orderedMap = new LinkedHashMap<>();

                for (Column col : getTable().filterColumns(column -> true)) {
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
                                        refMap.computeIfAbsent(ref.getName(), getFifoQueue()::selectRandom);
                                v = values.get(ref.getColumn());
                            }
                        } else {
                            v = getColumnGenerators().get(col).nextValue();
                        }
                    }
                    orderedMap.put(col.getName(), v);
                }

                topic.publishAsync(orderedMap);

                if (!consumer.consume(orderedMap, rowEstimate)) {
                    break;
                }
            }

            upstreamValues.clear();
            upstreamValues.putAll(getFifoQueue().take(each.getName()));
        }

        topic.publishAsync(Map.of()); // poison pill
    }
}

