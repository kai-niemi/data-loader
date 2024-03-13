package io.roach.volt.expression;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;

public class ExpressionGrammarTest {
    @Test
    void simpleExpression() {
        DefaultExpressionRegistry registry = new DefaultExpressionRegistry();
        registry.addVariable("pi", Math.PI);
        registry.addVariable("r", 25);

        registry.addFunction(FunctionDef.builder()
                .withId("pow")
                .withArgs(List.of("arg1", "arg2"))
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    Number arg2 = (Number) args[1];
                    return Math.pow(arg1.intValue(), arg2.intValue());
                }).build());

        Double result = VoltExpression.evaluate("2 * pi * r + pow(2,3)", Double.class, registry);
        Assertions.assertEquals(2 * Math.PI * 25 + Math.pow(2, 3), result);
    }

    @Test
    void sqlExpression() {
        final AtomicInteger rowNumber = new AtomicInteger();

        DefaultExpressionRegistry registry = new DefaultExpressionRegistry();
        registry.addFunction("rowOffset", args -> rowNumber.incrementAndGet());

        registry.addFunction(FunctionDef.builder()
                .withId("query")
                .withArgs(List.of("arg1", "arg2"))
                .withFunction(args -> {
                    final String query = (String) args[0];
                    final Object p1 = args[1];
                    return "ABC";
                }).build());

        IntStream.rangeClosed(1, 20).forEach(value -> {
            String result = VoltExpression.evaluate(
                    "query('SELECT full_name FROM users ORDER BY id OFFSET ? LIMIT 1', rowOffset() % 10)",
                    String.class, registry);
            System.out.printf("Result for #%d: %s\n", value, result);
        });
    }

    public static Stream<Arguments> arithmetics = Stream.of(
            Arguments.of(2, "1+1"),
            Arguments.of(-1, "1-2"),
            Arguments.of(4, "2*2"),
            Arguments.of(2, "4/2"),
            Arguments.of(4 % 2, "4%2"),
            Arguments.of((int) Math.pow(2, 3), "2^3"),
            Arguments.of(2 + 2, "2*1+ 2"),
            Arguments.of(2 * (1 + 2), "2*(1+2)"),
            Arguments.of(1 + (2 * 2), "1+ 2*2"),
            Arguments.of((1 + 2) * 2, "(1+ 2)*2"),
            Arguments.of((2 * (1 + 2)) * 2, "(2*(1+ 2))*2"),
            Arguments.of((2 * -(1 + 2)) * 2, "(2*-(1+ 2))*2"),
            Arguments.of((2) * 2, "(2*-(1+ -2))*2"),
            Arguments.of(1.5, "1.5"),
            Arguments.of(.5, "0.5"),
            Arguments.of(1.05 + 0.45, "1.05 + 0.45"),
            Arguments.of(5, "10 min 5"),
            Arguments.of(3, "10 min 5-2"),
            Arguments.of(3, "8 min 5-2"),
            Arguments.of(3, "(8 min 5)-2"),
            Arguments.of(10, "10 max 5"),
            Arguments.of(0, "2 mod 2"),
            Arguments.of(2, "2 % 4"),
            Arguments.of(0, "2 % 4-2"),
            Arguments.of(0, "(2 % 4)-2")
    );

    @ParameterizedTest
    @VariableSource("arithmetics")
    public void evaluateArithmetics(Object expected, String expression) {
        Object out = VoltExpression.evaluate(expression);
        Assertions.assertEquals(expected, out);
    }

    public static Stream<Arguments> stringOperations = Stream.of(
            Arguments.of("a", "'a'"),
            Arguments.of("ab", "'a' + 'b'"),
            Arguments.of("abc", "'a' + 'b' + 'c'")
    );

    @ParameterizedTest
    @VariableSource("stringOperations")
    public void evaluateStringOperations(Object expected, String expression) {
        Object out = VoltExpression.evaluate(expression);
        Assertions.assertEquals(expected, out);
    }

    public static Stream<Arguments> conditionals = Stream.of(
            Arguments.of(10, "if true then 10 else 20"),
            Arguments.of(20, "if false then 10 else 20"),
            Arguments.of("a", "if true then 'a' else 'b'"),
            Arguments.of("b", "if false then 'a' else 'b'"),
            Arguments.of(100, "if 1>0 then 100 else 200"),
            Arguments.of(100, "if 1>=0 then 100 else 200"),
            Arguments.of(100, "if 1>=1 then 100 else 200"),
            Arguments.of(200, "if 1>=2 then 100 else 200"),
            Arguments.of(100, "if 1<2 then 100 else 200"),
            Arguments.of(100, "if 1<=2 then 100 else 200"),
            Arguments.of(200, "if 1==2 then 100 else 200"),
            Arguments.of(100, "if 1==1 then 100 else 200"),
            Arguments.of(1, "if (1+2+3)*4 == 24 then 1 else 0"),
            Arguments.of(24, "if (1+2+3)*4 == 24 then (1+2+3)*4 else 0"),
            Arguments.of("yes", "if {d '2016-01-02'} > {d '2016-01-01'} then 'yes' else 'no'")
    );

    @ParameterizedTest
    @VariableSource("conditionals")
    public void evaluateConditionals(Object expected, String expression) {
        Object out = VoltExpression.evaluate(expression);
        Assertions.assertEquals(expected, out);
    }

    public static Stream<Arguments> conditionalsWithVariables = Stream.of(
            Arguments.of(2, "if x == y then 1 else 2"),
            Arguments.of(2, "if y == x then 1 else 2"),
            Arguments.of(1, "if x == 5 then 1 else 2"),
            Arguments.of(2, "if x == 10 then 1 else 2"),
            Arguments.of(1, "if x < y then 1 else 2"),
            Arguments.of(1, "if x <= y then 1 else 2"),
            Arguments.of(2, "if x > y then 1 else 2"),
            Arguments.of(2, "if x >= y then 1 else 2"),
            Arguments.of(2, "if -x >= y then 1 else 2"),
            Arguments.of(1, "if x >= -y then 1 else 2"),
            Arguments.of(1, "if -x >= -y then 1 else 2"),
            Arguments.of(2, "if x == y then 1 else 2"),
            Arguments.of(5.0, "if x != y then x else y"),
            Arguments.of(10.0, "if x != y then y else x"),
            Arguments.of(15.0, "if true then x+y else x-y"),
            Arguments.of(-5.0, "if false then x+y else x-y"),
            Arguments.of(1, "if 'a' in ('a','b','c') then 1 else 0"),
            Arguments.of(0, "if !'a' in ('a','b','c') then 1 else 0")
    );

    @ParameterizedTest
    @VariableSource("conditionalsWithVariables")
    public void evaluateConditionalWithVariables(Object expected, String expression) {
        DefaultExpressionRegistry map = new DefaultExpressionRegistry();
        map.addVariable("x", new BigDecimal("5.0"));
        map.addVariable("y", new BigDecimal("10.0"));

        Object out = VoltExpression.evaluate(expression, Object.class, map);
        Assertions.assertEquals(expected, out);
    }

    public static Stream<Arguments> literals = Stream.of(
            Arguments.of(true, "true"),
            Arguments.of(true, "TRUE"),
            Arguments.of(false, "false"),
            Arguments.of(false, "FALSE"),
            Arguments.of(1, "1"),
            Arguments.of(1.0, "1.0"),
            Arguments.of("abc", "'abc'"),
            Arguments.of(LocalDate.of(2021, 01, 01), "{d '2021-01-01'}"),
            Arguments.of(LocalTime.of(12, 00, 5), "{t '12:00:05'}"),
            Arguments.of(LocalDateTime.of(2021, 01, 01, 12, 0, 5), "{dt '2021-01-01 12:00:05'}")
    );

    @ParameterizedTest
    @VariableSource("literals")
    public void evaluateLiterals(Object expected, String expression) {
        Object out = VoltExpression.evaluate(expression);
        Assertions.assertEquals(expected, out);
    }

    // a=1.0
    // b=2.0
    // c=3.0
    // d=10.0
    // e=20
    public static Stream<Arguments> variableArithmetics = Stream.of(
            Arguments.of((-(2.0 + 3.0)) * 10.0, "(1.0 * -(2.0 + 3.0)) * 10.0"),
            Arguments.of((-(2.0 + 3.0)) * 10.0, "(a * -(b + c)) * d"),
            Arguments.of(10.0, "(a * -(b + -c)) * d"),
            Arguments.of(10.0, "(a*-(b+-c))*d"),
            Arguments.of(50.0, "(a*(b+ c))*d"),
            Arguments.of(10.0, "(1.0*-(2.0+ -3.0))*10.0"),
            Arguments.of(3.0, "a+b"),
            Arguments.of(-1.0, "a-b"),
            Arguments.of(2.0, "a*b"),
            Arguments.of(0.5, "a/b"),
            Arguments.of((double) 1 % 2, "a%b"),
            Arguments.of(Math.pow(1, 2), "a^b"),
            Arguments.of((double) 1 * 2 + 3, "a*b+c"),
            Arguments.of((double) 1 * (2 + 3), "a*(b+c)"),
            Arguments.of((double) 1 + (2 * 3), "a+b*c"),
            Arguments.of(((double) 1 + 2) * 3, "(a+b)*c"),
            Arguments.of(10.0, "d"),
            Arguments.of(20.0, "e"),
            Arguments.of(1.0, "a min b"),
            Arguments.of(-1.0, "a min b-c"),
            Arguments.of(-2.0, "(a min b)-c"),
            Arguments.of(2.0, "a max b"),
            Arguments.of(1.0, "a mod b"),
            Arguments.of(0.0, "a % b-c"),
            Arguments.of(-2.0, "(a % b)-c")
    );

    @ParameterizedTest
    @VariableSource("variableArithmetics")
    public void evaluateVariableArithmetics(Object expected, String expression) {
        DefaultExpressionRegistry map = new DefaultExpressionRegistry();
        map.addVariable("a", new BigDecimal("1.0"));
        map.addVariable("b", new BigDecimal("2.0"));
        map.addVariable("c", new BigDecimal("3.0"));
        map.addVariable("d", new BigDecimal("10.0"));
        map.addVariable("e", new BigDecimal("20.0"));

        Double right = VoltExpression.evaluate(expression, Double.class, map);
        Assertions.assertEquals(expected, right, expression);
    }

    public static Stream<Arguments> functionArithmetics = Stream.of(
            Arguments.of("Alice Donald", "femaleFirstName() + ' ' + lastName()"),
            Arguments.of("Alice", "if true then femaleFirstName() otherwise maleFirstName()"),
            Arguments.of("Bob", "if false then femaleFirstName() otherwise maleFirstName()"),
            Arguments.of("bar", "if rowNumber() % 2 == 0 then foo() otherwise bar()"),
            Arguments.of("foo", "if rowNumber() % 2 == 0 then foo() otherwise bar()"),
            Arguments.of("bar", "if rowNumber() % 2 == 0 then foo() otherwise bar()"),
            Arguments.of("foo", "if rowNumber() % 2 == 0 then foo() otherwise bar()"),
            Arguments.of("Alice Donald",
                    "if 2 % 2 == 0 then femaleFirstName() + ' ' + lastName() otherwise maleFirstName() + ' ' + lastName()"),
            Arguments.of("Bob Donald",
                    "if 3 % 2 == 0 then femaleFirstName() + ' ' + lastName() otherwise maleFirstName() + ' ' + lastName()")
    );

    private static final AtomicInteger counter = new AtomicInteger();

    @ParameterizedTest
    @VariableSource("functionArithmetics")
    public void evaluateFunctionArithmetics(Object expected, String expression) {
        DefaultExpressionRegistry map = new DefaultExpressionRegistry();
        map.addFunction("rowNumber", args -> {
            return counter.incrementAndGet();
        });
        map.addFunction("foo", args -> "foo");
        map.addFunction("bar", args -> "bar");
        map.addFunction(FunctionDef.builder()
                .withId("femaleFirstName")
                .withFunction(args -> {
                    return "Alice";
                }).build());
        map.addFunction(FunctionDef.builder()
                .withId("maleFirstName")
                .withFunction(args -> {
                    return "Bob";
                }).build());
        map.addFunction(FunctionDef.builder()
                .withId("lastName")
                .withFunction(args -> {
                    return "Donald";
                }).build());

        Object out = VoltExpression.evaluate(expression, Object.class, map);
        Assertions.assertEquals(expected, out);
    }

    public static Stream<Arguments> functions = Stream.of(
            Arguments.of("bar", "query('SELECT full_name FROM users ORDER BY id OFFSET ? LIMIT 1')"),
            Arguments.of(Math.pow(3, Math.pow(2, 1 + 2)), "pow(3,pow(2,1+2))"),
            Arguments.of(Math.PI, "pi"),
            Arguments.of(Math.PI * 2, "pi * 2"),
            Arguments.of(-Math.PI * 2, "-pi * 2"),
            Arguments.of(1.0, "one()"),
            Arguments.of(1 + 2, "onePlus(2)"),
            Arguments.of(-Math.PI, "negate(pi)"),
            Arguments.of(Math.pow(2, 3), "pow(2,3)"),
            Arguments.of(Math.pow(-(4 + 3) * 2, 2), "pow(negate((4+3)*2),2)"),
            Arguments.of(-(Math.PI + Math.PI * 2 + -Math.PI), "negate(add(pi,pi*2,negate(pi)))")
    );

    @ParameterizedTest
    @VariableSource("functions")
    public void evaluateFunctions(Object expected, String expression) {
        DefaultExpressionRegistry registry = new DefaultExpressionRegistry();
        registry.addVariable("pi", Math.PI);
        registry.addFunction("one", args -> {
            return 1.0;
        });

        registry.addFunction(FunctionDef.builder()
                .withId("onePlus")
                .withArgs(List.of("arg1"))
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    return 1 + arg1.intValue();
                }).build());
        registry.addFunction(FunctionDef.builder()
                .withId("negate")
                .withArgs(List.of("arg1"))
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    return -arg1.doubleValue();
                }).build());
        registry.addFunction(FunctionDef.builder()
                .withId("pow")
                .withArgs(List.of("arg1", "arg2"))
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    Number arg2 = (Number) args[1];
                    return Math.pow(arg1.intValue(), arg2.intValue());
                }).build());
        registry.addFunction(FunctionDef.builder()
                .withId("add")
                .withArgs(List.of("arg1", "arg2", "arg3"))
                .withFunction(args -> {
                    Number arg1 = (Number) args[0];
                    Number arg2 = (Number) args[1];
                    Number arg3 = (Number) args[2];
                    return arg1.doubleValue() + arg2.doubleValue() + arg3.doubleValue();
                }).build());
        registry.addFunction(FunctionDef.builder()
                .withId("query")
                .withArgs(List.of("arg1"))
                .withFunction(args -> {
                    return "bar";
                }).build());

        Object out = VoltExpression.evaluate(expression, Object.class, registry);
        Assertions.assertEquals(expected, out);
    }

    public static Stream<Arguments> illegalExpressions = Stream.of(
            Arguments.of(false, "132.2b"),
            Arguments.of(false, ")"),
            Arguments.of(false, "(1))"),
            Arguments.of(false, "1+"),
            Arguments.of(false, "1-"),
            Arguments.of(false, "1/"),
            Arguments.of(false, "1*"),
            Arguments.of(false, "1^"),
            Arguments.of(false, "*1"),
            Arguments.of(false, "/1"),
            Arguments.of(false, "'a'+1"),
            Arguments.of(false, "1+'a'"),
            Arguments.of(false, "-'a'"),
            Arguments.of(false, "(1"),
            Arguments.of(false, "1)"),
            Arguments.of(false, "(1*(1"),
            Arguments.of(false, "false+1"),
            Arguments.of(false, "1+false "),
            Arguments.of(false, "if 'a' in (1,2,3) then 1 else 0"),
            Arguments.of(false, "if 1 in (1,2,3) then 1 else 0")
    );

    @ParameterizedTest
    @VariableSource("illegalExpressions")
    public void evaluateIllegals(Object expected, String expression) {
        Assertions.assertEquals(expected, VoltExpression.isValid(expression));
    }
}
