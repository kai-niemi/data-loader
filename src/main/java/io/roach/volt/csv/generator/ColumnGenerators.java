package io.roach.volt.csv.generator;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Gen;
import io.roach.volt.csv.model.Range;
import io.roach.volt.csv.model.ValueSet;
import io.roach.volt.expression.ExpressionRegistry;
import io.roach.volt.util.RandomData;

import javax.sql.DataSource;
import java.time.temporal.Temporal;

public abstract class ColumnGenerators {
    private ColumnGenerators() {
    }

    public static ColumnGenerator<? extends Temporal> createRangeGenerator(Range range) {
        return switch (range.getType()) {
            case date -> new LocalDateRangeGenerator(range);
            case time -> new LocalTimeRangeGenerator(range);
            case datetime -> new LocalDateTimeGenerator(range);
        };
    }

    public static ColumnGenerator<?> createIdGenerator(Gen gen, DataSource dataSource) {
        return switch (gen.getType()) {
            case uuid -> new UUIDGenerator();
            case sequence -> new SequenceGenerator(gen);
            case database_sequence -> new DatabaseSequenceGenerator(dataSource, gen);
            case ordered, unordered -> new RowIdGenerator(dataSource, gen);
        };
    }

    public static ColumnGenerator<?> createConstantGenerator(Column column) {
        return column::getConstant;
    }

    public static ColumnGenerator<?> createExpressionGenerator(Column column, ExpressionRegistry registry) {
        return new ExpressionGenerator(column, registry);
    }

    public static ColumnGenerator<?> createValueSetGenerator(ValueSet<?> valueSet) {
        return () -> {
            if (valueSet.getWeights().isEmpty()) {
                return RandomData.selectRandom(valueSet.getValues());
            }
            return RandomData.selectRandomWeighted(valueSet.getValues(), valueSet.getWeights());
        };
    }
}
