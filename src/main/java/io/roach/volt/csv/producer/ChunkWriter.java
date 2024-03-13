package io.roach.volt.csv.producer;

import io.roach.volt.csv.model.Column;
import org.springframework.batch.item.Chunk;

import java.util.List;

public interface ChunkWriter<W> {
    void open(List<Column> columns);

    void writeChunk(Chunk<? extends W> values);

    void close();
}
