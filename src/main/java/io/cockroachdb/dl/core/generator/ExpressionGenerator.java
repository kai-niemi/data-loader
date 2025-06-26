package io.cockroachdb.dl.core.generator;

import io.cockroachdb.dl.core.model.Column;
import io.cockroachdb.dl.expression.ExpressionRegistry;
import io.cockroachdb.dl.expression.Expression;
import org.springframework.util.StringUtils;

public class ExpressionGenerator implements ValueGenerator<Object> {
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
