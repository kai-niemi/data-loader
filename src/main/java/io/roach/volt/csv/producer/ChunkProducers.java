package io.roach.volt.csv.producer;

import io.roach.volt.csv.TableConfigException;
import io.roach.volt.csv.generator.ColumnGenerator;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Table;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public abstract class ChunkProducers {
    private ChunkProducers() {
    }

    public static List<ProducerBuilder> options() {
        return List.of(
                new CrossProductProducer(),
                new DownStreamProducer(),
                new UpStreamProducer()
        );
    }

    public interface ProducerBuilder extends Predicate<Table> {
        ChunkProducer<String, Object> createChunkProducer(Table table,
                                                          Map<Column, ColumnGenerator<?>> generatorMap,
                                                          int queueCapacity);

        void validate(Table table);
    }

    public static class CrossProductProducer implements ProducerBuilder {
        @Override
        public boolean test(Table table) {
            long eachCols = table.filterColumns(Table.WITH_EACH).size();
            return eachCols > 1;
        }

        @Override
        public void validate(Table table) {
            if (table.getFinalCount() != 0) {
                throw new TableConfigException("Expected row count zero (0) for cross product producer but got ("
                        + table.getFinalCount() + ")", table);
            }

            Set<String> topics = new HashSet<>();

            table.filterColumns(Table.WITH_EACH)
                    .stream()
                    .map(Column::getEach)
                    .forEach(each -> {
                        if (each.getMultiplier() <= 0) {
                            throw new TableConfigException("Expected multiplier > 0, got " + each, table);
                        }

                        if (!topics.add(each.getName())) {
                            throw new TableConfigException("Expected unique 'each' columns in cross product producer: " + each, table);
                        }
                    });


            if (topics.size() < 2) {
                throw new TableConfigException("Expected at least two 'each' columns in cross product producer but got " + topics.size(), table);
            }
        }

        @Override
        public ChunkProducer<String, Object> createChunkProducer(Table table,
                                                                 Map<Column, ColumnGenerator<?>> generatorMap,
                                                                 int queueCapacity) {
            return new CrossProductChunkProducer(table, generatorMap, queueCapacity);
        }
    }

    public static class DownStreamProducer implements ProducerBuilder {
        @Override
        public boolean test(Table table) {
            long eachCols = table.filterColumns(Table.WITH_EACH).size();
            return eachCols == 1 && table.getFinalCount() == 0;
        }

        @Override
        public void validate(Table table) {
            if (table.getFinalCount() != 0) {
                throw new TableConfigException("Expected row count zero for downstream producer", table);
            }

            Set<String> topics = new HashSet<>();

            table.filterColumns(Table.WITH_EACH)
                    .stream()
                    .map(Column::getEach)
                    .forEach(each -> {
                        if (each.getMultiplier() <= 0) {
                            throw new TableConfigException("Expected multiplier > 0, got " + each, table);
                        }
                        topics.add(each.getName());
                    });

            if (topics.size() != 1) {
                throw new TableConfigException("Expected exactly one 'each' column for downstream producer but got " + topics.size(), table);
            }
        }

        @Override
        public ChunkProducer<String, Object> createChunkProducer(Table table,
                                                                 Map<Column, ColumnGenerator<?>> generatorMap,
                                                                 int queueCapacity) {
            return new DownstreamChunkProducer(table, generatorMap, queueCapacity);
        }
    }

    public static class UpStreamProducer implements ProducerBuilder {
        @Override
        public boolean test(Table table) {
            long eachCols = table.filterColumns(Table.WITH_EACH).size();
            return eachCols == 0 && table.getFinalCount() > 0;
        }

        @Override
        public void validate(Table table) {
            if (table.getFinalCount() <= 0) {
                throw new TableConfigException("Expected row count > 0 for upstream producer", table);
            }

            if (!table.filterColumns(Table.WITH_EACH).isEmpty()) {
                throw new TableConfigException("Expected zero 'each' columns for upstream producer", table);
            }
        }

        @Override
        public ChunkProducer<String, Object> createChunkProducer(Table table,
                                                                 Map<Column, ColumnGenerator<?>> generatorMap,
                                                                 int queueCapacity) {
            return new UpstreamChunkProducer(table, generatorMap, queueCapacity);
        }
    }
}
