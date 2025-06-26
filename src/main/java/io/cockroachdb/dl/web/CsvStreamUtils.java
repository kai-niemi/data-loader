package io.cockroachdb.dl.web;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.sql.DataSource;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;

import io.cockroachdb.dl.core.generator.ValueGenerator;
import io.cockroachdb.dl.core.generator.ValueGenerators;
import io.cockroachdb.dl.core.model.Column;
import io.cockroachdb.dl.core.model.Table;
import io.cockroachdb.dl.core.stream.CsvStreamWriter;
import io.cockroachdb.dl.core.stream.CsvStreamWriterBuilder;
import io.cockroachdb.dl.expression.ExpressionRegistry;
import io.cockroachdb.dl.expression.ExpressionRegistryBuilder;
import io.cockroachdb.dl.expression.FunctionDef;
import io.cockroachdb.dl.web.model.TableModel;

public abstract class CsvStreamUtils {
    private CsvStreamUtils() {
    }

    private static final Predicate<Column> COLUMN_INCLUDE_PREDICATE
            = column -> (column.isHidden() == null || !column.isHidden())
                        && column.getEach() == null && column.getRef() == null;

    public static void writeCsvStream(DataSource dataSource,
                                      TableModel tableModel,
                                      OutputStream outputStream) {
        Table table = new Table();
        table.setName(tableModel.getTable());
        table.setCount(tableModel.getRows());
        table.setColumns(tableModel.getColumns());

        String refs = table.filterColumns(Table.WITH_REF.or(Table.WITH_EACH))
                .stream()
                .map(Column::getName)
                .collect(Collectors.joining());

        if (!refs.isEmpty() && !tableModel.isIgnoreForeignKeys()) {
            throw new IllegalStateException("Table contains foreign key column(s): " + refs);
        }

        final AtomicInteger currentRow = new AtomicInteger();

        final Map<Column, ValueGenerator<?>> columnGenerators
                = createColumnGenerators(dataSource, table, List.of(FunctionDef.builder()
                .withCategory("other")
                .withId("rowNumber")
                .withDescription("Returns current row number.")
                .withReturnValue(Integer.class)
                .withFunction(args -> currentRow.get())
                .build()));

        try (CsvStreamWriter<Map<String, Object>> writer = new CsvStreamWriterBuilder<Map<String, Object>>()
                .withDelimiter(tableModel.getDelimiter())
                .withQuoteCharacter(tableModel.getQuoteCharacter())
                .withIncludeHeader(tableModel.isIncludeHeader())
                .withColumnNames(table
                        .filterColumns(COLUMN_INCLUDE_PREDICATE)
                        .stream()
                        .map(Column::getName)
                        .toList())
                .build()) {

            if (tableModel.isGzip()) {
                writer.setWriter(new BufferedWriter(
                        new OutputStreamWriter(new GZIPOutputStream(outputStream, true))));
            } else {
                writer.setWriter(new BufferedWriter(
                        new OutputStreamWriter(outputStream)));
            }

            writer.open(new ExecutionContext());

            for (int i = 0; i < table.getFinalCount(); i++) {
                Map<String, Object> orderedTuples = new LinkedHashMap<>();
                for (Column col : table.filterColumns(COLUMN_INCLUDE_PREDICATE)) {
                    orderedTuples.put(col.getName(), columnGenerators.get(col).nextValue());
                }
                writer.write(Chunk.of(orderedTuples));
                currentRow.incrementAndGet();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<Column, ValueGenerator<?>> createColumnGenerators(DataSource dataSource,
                                                                         Table table,
                                                                         List<FunctionDef> functionDefs) {
        ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);
        functionDefs.forEach(registry::addFunction);

        Map<Column, ValueGenerator<?>> generatorMap = new HashMap<>();

        // Get generator for all non-ref columns
        table.filterColumns(Table.WITH_REF.or(Table.WITH_EACH).negate())
                .forEach(column -> generatorMap.put(column,
                        ValueGenerators.createValueGenerator(column, dataSource, registry)));

        return generatorMap;
    }
}
