package io.cockroachdb.volt.csv.file;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.cockroachdb.volt.csv.model.Table;

/**
 * Qualifier for table chunk producers, typically mapped by inspecting
 * table configurations. A table can only have one type of chunk producer.
 */
public interface ChunkProducerQualifier
        extends Predicate<Table>, Supplier<ChunkProducer<String,Object>> {
    String description();

    void validate(List<Table> allTables, Table table);
}
