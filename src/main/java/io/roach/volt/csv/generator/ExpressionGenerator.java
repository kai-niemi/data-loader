package io.roach.volt.csv.generator;

import io.roach.volt.csv.model.Column;
import io.roach.volt.expression.ExpressionRegistry;
import io.roach.volt.expression.Expression;
import org.springframework.util.StringUtils;

public class ExpressionGenerator implements ColumnGenerator<Object> {
    private final Column column;

    private final ExpressionRegistry registry;

    public ExpressionGenerator(Column column, ExpressionRegistry registry) {
        this.column = column;
        this.registry = registry;
    }

    @Override
    public Object nextValue() {
        String expression = column.getExpression();
        if (StringUtils.hasLength(expression)) {
            return Expression.evaluate(expression, Object.class, registry);
        }
        throw new IllegalStateException("Undefined column value generator for: "
                + column.getName());
    }

}
