package io.cockroachdb.volt.csv.generator;

import java.time.temporal.Temporal;

import javax.sql.DataSource;

import org.springframework.util.StringUtils;

import io.cockroachdb.volt.csv.ConfigurationException;
import io.cockroachdb.volt.csv.model.Column;
import io.cockroachdb.volt.csv.model.Gen;
import io.cockroachdb.volt.csv.model.Range;
import io.cockroachdb.volt.csv.model.ValueSet;
import io.cockroachdb.volt.expression.ExpressionRegistry;
import io.cockroachdb.volt.util.RandomData;

public abstract class ValueGenerators {
    private ValueGenerators() {
    }

    public static ValueGenerator<?> createValueGenerator(Column column,
                                                         DataSource dataSource,
                                                         ExpressionRegistry expressionRegistry) {
        if (column.getRange() != null) {
            return ValueGenerators.createRangeGenerator(column.getRange());
        }

        Gen gen = column.getGen();
        if (gen != null) {
            return ValueGenerators.createIdGenerator(gen, dataSource);
        }

        String constant = column.getConstant();
        if (StringUtils.hasLength(constant)) {
            return ValueGenerators.createConstantGenerator(column);
        }

        if (StringUtils.hasLength(column.getExpression())) {
            return ValueGenerators.createExpressionGenerator(column, expressionRegistry);
        }

        ValueSet<?> set = column.getSet();
        if (set != null) {
            return ValueGenerators.createValueSetGenerator(set);
        }

        throw new ConfigurationException("No column value generator found", column);
    }


    public static ValueGenerator<? extends Temporal> createRangeGenerator(Range range) {
        return switch (range.getType()) {
            case date -> new LocalDateRangeGenerator(range);
            case time -> new LocalTimeRangeGenerator(range);
            case datetime -> new LocalDateTimeGenerator(range);
        };
    }

    public static ValueGenerator<?> createIdGenerator(Gen gen, DataSource dataSource) {
        return switch (gen.getType()) {
            case uuid -> new UUIDGenerator();
            case sequence -> new SequenceGenerator(gen);
            case database_sequence -> new DatabaseSequenceGenerator(dataSource, gen);
            case ordered, unordered -> new RowIdGenerator(dataSource, gen);
        };
    }

    public static ValueGenerator<?> createConstantGenerator(Column column) {
        return column::getConstant;
    }

    public static ValueGenerator<?> createExpressionGenerator(Column column, ExpressionRegistry registry) {
        return new ExpressionGenerator(column, registry);
    }

    public static ValueGenerator<?> createValueSetGenerator(ValueSet<?> valueSet) {
        return () -> {
            if (valueSet.getWeights().isEmpty()) {
                return RandomData.selectRandom(valueSet.getValues());
            }
            return RandomData.selectRandomWeighted(valueSet.getValues(), valueSet.getWeights());
        };
    }
}
