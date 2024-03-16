package io.roach.volt.shell;

import io.roach.volt.expression.ExpressionException;
import io.roach.volt.expression.ExpressionRegistry;
import io.roach.volt.expression.ExpressionRegistryBuilder;
import io.roach.volt.expression.VoltExpression;
import io.roach.volt.shell.support.AnsiConsole;
import io.roach.volt.shell.support.FunctionValueProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;

@ShellComponent
@ShellCommandGroup(CommandGroups.EXPR)
public class Expression {
    @Autowired
    private DataSource dataSource;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Evaluate expression", key = {"expr-eval", "e"})
    public void evaluateFunction(@ShellOption(
            value = {"expression"},
            valueProvider = FunctionValueProvider.class) String expression) {

        final ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);

        ansiConsole.magenta("Expression: ")
                .green("%s", expression)
                .nl();

        try {
            Instant now = Instant.now();
            Object result = VoltExpression.evaluate(expression, registry);
            Duration duration = Duration.between(now, Instant.now());

            ansiConsole.magenta("Result: ")
                    .green("%s", result)
                    .yellow(" (%s)\n", result.getClass().getName())
                    .nl()
                    .magenta("Time: ")
                    .green(" %s", duration.toString())
                    .nl();
        } catch (ExpressionException e) {
            ansiConsole.red("ERROR: %s", e.getMessage()).nl();
        }
    }

    @ShellMethod(value = "List expression functions and variables", key = {"expr-functions", "lf"})
    public void listFunctions() {
        final ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);

        ansiConsole.cyan("-- Functions --").nl();
        registry.functionDefinitions().forEach(fn -> {
            ansiConsole.magenta("%s", fn.toString()).nl();
        });
        ansiConsole.cyan("-- Variables --").nl();
        registry.variableNames().forEach(fn -> {
            ansiConsole.magenta("%s", fn).nl();
        });
    }
}
