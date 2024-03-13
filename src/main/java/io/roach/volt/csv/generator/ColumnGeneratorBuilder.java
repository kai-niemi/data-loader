package io.roach.volt.csv.generator;

import io.roach.volt.csv.ModelConfigException;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Gen;
import io.roach.volt.csv.model.ValueSet;
import io.roach.volt.expression.ExpressionRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;

@Component
public class ColumnGeneratorBuilder {
    @Autowired
    private DataSource dataSource;

    public ColumnGenerator<?> createColumnGenerator(Column column,
                                                    ExpressionRegistry expressionRegistry) {
        if (column.getRange() != null) {
            return ColumnGenerators.createRangeGenerator(column.getRange());
        }

        Gen gen = column.getGen();
        if (gen != null) {
            return ColumnGenerators.createIdGenerator(gen, dataSource);
        }

        String constant = column.getConstant();
        if (StringUtils.hasLength(constant)) {
            return ColumnGenerators.createConstantGenerator(column);
        }

        if (StringUtils.hasLength(column.getExpression())) {
            return ColumnGenerators.createExpressionGenerator(column, expressionRegistry);
        }

        ValueSet<?> set = column.getSet();
        if (set != null) {
            return ColumnGenerators.createValueSetGenerator(set);
        }

        throw new ModelConfigException("No column generator for: " + column.getName());
    }
}
