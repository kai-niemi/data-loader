package io.cockroachdb.volt.csv.file;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.cockroachdb.volt.csv.ConfigurationException;
import io.cockroachdb.volt.csv.model.Column;
import io.cockroachdb.volt.csv.model.Each;
import io.cockroachdb.volt.csv.model.Ref;
import io.cockroachdb.volt.csv.model.Table;

public abstract class ChunkProducers {
    private ChunkProducers() {
    }

    public static List<ChunkProducerQualifier> allQualifiers() {
        return List.of(
                new Cartesian(),
                new DownStream(),
                new UpStream()
        );
    }

    private static void validateRef(List<Table> allTables, Table table, Ref ref) {
        allTables.stream()
                .filter(t -> t.getName().equals(ref.getName()))
                .findAny()
                .orElseThrow(() ->
                        new ConfigurationException("Ref table '%s' not found"
                                .formatted(ref.getName()), table))
                .getColumns().stream()
                .filter(column -> column.getName().equals(ref.getColumn()))
                .findAny()
                .orElseThrow(() ->
                        new ConfigurationException("Ref column '%s' not found in table '%s'"
                                .formatted(ref.getColumn(), ref.getName()), table));
    }

    private static void validateEachRef(List<Table> allTables, Table table, Each each) {
        allTables.stream()
                .filter(t -> t.getName().equals(each.getName()))
                .findAny()
                .orElseThrow(() ->
                        new ConfigurationException("Each ref table '%s' not found"
                                .formatted(each.getName()), table))
                .getColumns().stream()
                .filter(column -> column.getName().equals(each.getColumn()))
                .findAny()
                .orElseThrow(() ->
                        new ConfigurationException("Each ref column '%s' not found in table '%s'"
                                .formatted(each.getColumn(), each.getName()), table));
    }

    public static class Cartesian implements ChunkProducerQualifier {
        @Override
        public String description() {
            return "cartesian producer";
        }

        @Override
        public boolean test(Table table) {
            long eachCols = table.filterColumns(Table.WITH_EACH).size();
            return eachCols > 1;
        }

        @Override
        public void validate(List<Table> allTables, Table table) {
            if (table.getFinalCount() != 0) {
                throw new ConfigurationException("Expected row count zero (0) for cross product producer but got ("
                        + table.getFinalCount() + ")", table);
            }

            Set<String> topics = new HashSet<>();

            table.filterColumns(Table.WITH_EACH)
                    .stream()
                    .map(Column::getEach)
                    .forEach(each -> {
                        validateEachRef(allTables, table, each);

                        if (each.getMultiplier() <= 0) {
                            throw new ConfigurationException("Expected multiplier > 0, got " + each, table);
                        }

                        if (!topics.add(each.getName())) {
                            throw new ConfigurationException(
                                    "Expected unique 'each' columns in cross product producer: " + each, table);
                        }
                    });


            if (topics.size() < 2) {
                throw new ConfigurationException(
                        "Expected at least two 'each' columns in cross product producer but got " + topics.size(),
                        table);
            }
        }

        @Override
        public AsyncChunkProducer get() {
            return new CartesianChunkProducer();
        }
    }

    public static class DownStream implements ChunkProducerQualifier {
        @Override
        public String description() {
            return "downstream producer";
        }

        @Override
        public boolean test(Table table) {
            long eachCols = table.filterColumns(Table.WITH_EACH).size();
            return eachCols == 1 && table.getFinalCount() == 0;
        }

        @Override
        public void validate(List<Table> allTables, Table table) {
            if (table.getFinalCount() != 0) {
                throw new ConfigurationException("Expected row count zero for downstream producer", table);
            }

            Set<String> topics = new HashSet<>();

            table.filterColumns(Table.WITH_EACH)
                    .stream()
                    .map(Column::getEach)
                    .forEach(each -> {
                        validateEachRef(allTables, table, each);

                        if (each.getMultiplier() <= 0) {
                            throw new ConfigurationException("Expected multiplier > 0, got " + each, table);
                        }
                        topics.add(each.getName());
                    });

            table.filterColumns(Table.WITH_REF)
                    .stream()
                    .map(Column::getRef)
                    .forEach(ref -> validateRef(allTables, table, ref));

            if (topics.size() != 1) {
                throw new ConfigurationException(
                        "Expected exactly one 'each' column for downstream producer but got " + topics.size(), table);
            }
        }

        @Override
        public AsyncChunkProducer get() {
            return new DownstreamChunkProducer();
        }
    }

    public static class UpStream implements ChunkProducerQualifier {
        @Override
        public String description() {
            return "upstream producer";
        }

        @Override
        public boolean test(Table table) {
            long eachCols = table.filterColumns(Table.WITH_EACH).size();
            return eachCols == 0 && table.getFinalCount() > 0;
        }

        @Override
        public void validate(List<Table> allTables, Table table) {
            if (table.getFinalCount() <= 0) {
                throw new ConfigurationException("Expected row count > 0 for upstream producer", table);
            }

            if (!table.filterColumns(Table.WITH_EACH).isEmpty()) {
                throw new ConfigurationException("Expected zero 'each' columns for upstream producer", table);
            }

            table.filterColumns(Table.WITH_REF)
                    .stream()
                    .map(Column::getRef)
                    .forEach(ref -> validateRef(allTables, table, ref));
        }

        @Override
        public AsyncChunkProducer get() {
            return new UpstreamChunkProducer();
        }
    }
}
