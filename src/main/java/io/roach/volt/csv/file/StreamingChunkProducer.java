package io.roach.volt.csv.file;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.roach.volt.csv.generator.ValueGenerator;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Table;

public class StreamingChunkProducer implements ChunkProducer<String, Object> {
    private final Table table;

    private final Map<Column, ValueGenerator<?>> columnGenerators;

    private final Predicate<Column> predicate;

    private final AtomicInteger currentRow = new AtomicInteger();

    public StreamingChunkProducer(Table table,
                                  Map<Column, ValueGenerator<?>> columnGenerators,
                                  Predicate<Column> predicate) {
        this.columnGenerators = columnGenerators;
        this.table = table;
        this.predicate = predicate;

        for (Column col : table.filterColumns(predicate)) {
            if (!columnGenerators.containsKey(col)) {
                throw new IllegalStateException("Missing column generator for: " + col);
            }
        }
    }

    @Override
    public Supplier<Integer> currentRow() {
        return currentRow::get;
    }

    @Override
    public void produceChunks(ChunkConsumer<String, Object> consumer) throws Exception {
        for (int i = 0; i < table.getFinalCount(); i++) {
            currentRow.incrementAndGet();

            Map<String, Object> orderedTuples = new LinkedHashMap<>();

            for (Column col : table.filterColumns(predicate)) {
                orderedTuples.put(col.getName(), columnGenerators.get(col).nextValue());
            }

            if (!consumer.consumeChunk(orderedTuples, table.getFinalCount())) {
                break;
            }
        }
    }
}
