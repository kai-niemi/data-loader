package io.roach.volt.expression;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A mutable variable resolver that wraps a standard Map collection.
 *
 * @author Kai Niemi
 */
public class DefaultExpressionRegistry implements ExpressionRegistry {
    private final Map<String, Object> variables = new HashMap<>();

    private final Map<String, FunctionDef> functions = new HashMap<>();

    @Override
    public DefaultExpressionRegistry addVariable(String id, Object value) {
        this.variables.putIfAbsent(id, value);
        return this;
    }

    @Override
    public DefaultExpressionRegistry addFunction(String id, Function function) {
        return addFunction(FunctionDef.builder()
                .withId(id)
                .withFunction(function)
                .build());
    }

    @Override
    public DefaultExpressionRegistry addFunction(FunctionDef functionDef) {
        Arrays.stream(functionDef
                .getId().split(";")).sequential().forEach(k -> this.functions.putIfAbsent(k, functionDef));
        return this;
    }

    @Override
    public Optional<Object> findVariable(String id) {
        return Optional.ofNullable(variables.get(id));
    }

    @Override
    public Optional<FunctionDef> findFunction(String id) {
        return Optional.ofNullable(functions.get(id));
    }

    @Override
    public Iterable<String> variableNames() {
        return variables.keySet();
    }

    @Override
    public Iterable<FunctionDef> functionDefinitions() {
        return functions.values();
    }
}
