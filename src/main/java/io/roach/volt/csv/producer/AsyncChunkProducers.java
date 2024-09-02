package io.roach.volt.csv.producer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.roach.volt.csv.TableConfigException;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Table;

public abstract class AsyncChunkProducers {
    private AsyncChunkProducers() {
    }

    public static List<ProducerFactory> options() {
        return List.of(
                new Cartesian(),
                new DownStream(),
                new UpStream()
        );
    }

    public interface ProducerFactory extends Predicate<Table>, Supplier<AsyncChunkProducer> {
        void validate(Table table);
    }

    public static class Cartesian implements ProducerFactory {
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
                            throw new TableConfigException(
                                    "Expected unique 'each' columns in cross product producer: " + each, table);
                        }
                    });


            if (topics.size() < 2) {
                throw new TableConfigException(
                        "Expected at least two 'each' columns in cross product producer but got " + topics.size(),
                        table);
            }
        }

        @Override
        public AsyncChunkProducer get() {
            return new CrossProductChunkProducer();
        }
    }

    public static class DownStream implements ProducerFactory {
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
                throw new TableConfigException(
                        "Expected exactly one 'each' column for downstream producer but got " + topics.size(), table);
            }
        }

        @Override
        public AsyncChunkProducer get() {
            return new DownstreamChunkProducer();
        }
    }

    public static class UpStream implements ProducerFactory {
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
        public AsyncChunkProducer get() {
            return new UpstreamChunkProducer();
        }
    }
}
