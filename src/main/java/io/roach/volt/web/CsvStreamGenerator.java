package io.roach.volt.web;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.sql.DataSource;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import io.roach.volt.csv.file.ChunkProducer;
import io.roach.volt.csv.file.StreamingChunkProducer;
import io.roach.volt.csv.generator.ValueGenerator;
import io.roach.volt.csv.generator.ValueGenerators;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Table;
import io.roach.volt.csv.stream.CsvStreamWriter;
import io.roach.volt.csv.stream.CsvStreamWriterBuilder;
import io.roach.volt.expression.ExpressionRegistry;
import io.roach.volt.expression.ExpressionRegistryBuilder;
import io.roach.volt.expression.FunctionDef;

@Component
public class CsvStreamGenerator {
    private static final Predicate<Column> COLUMN_INCLUDE_PREDICATE
            = column -> (column.isHidden() == null || !column.isHidden())
            && column.getEach() == null && column.getRef() == null;

    @Autowired
    private DataSource dataSource;

    public String generateImportInto(TableModel tableModel) {
        String uri = ServletUriComponentsBuilder.fromCurrentContextPath()
                .pathSegment("table", "schema", tableModel.getTable())
                .buildAndExpand()
                .toUriString();

        StringBuilder sb = new StringBuilder();
        sb.append("IMPORT INTO ")
                .append(tableModel.getTable())
                .append("(")
                .append(tableModel.getColumns()
                        .stream()
                        .map(Column::getName)
                        .collect(Collectors.joining(",")))
                .append(") CSV DATA ( '").append(uri).append("');");
        return sb.toString();
    }

    public void generateCsvStream(TableModel tableModel, OutputStream outputStream) {
        Table table = new Table();
        table.setName(tableModel.getTable());
        table.setCount(tableModel.getRows());
        table.setColumns(tableModel.getColumns());

        String refs = table.filterColumns(Table.WITH_REF.or(Table.WITH_EACH))
                .stream()
                .map(Column::getName)
                .collect(Collectors.joining());

        if (!refs.isEmpty() && !tableModel.isIgnoreForeignKeys()) {
            throw new BadRequestException("Table contains foreign key column(s): " + refs);
        }

        final AtomicInteger currentRow = new AtomicInteger();

        final Map<Column, ValueGenerator<?>> columnGenerators
                = createColumnGenerators(table, List.of(FunctionDef.builder()
                .withCategory("other")
                .withId("rowNumber")
                .withDescription("Returns current row number in CSV file.")
                .withReturnValue(Integer.class)
                .withFunction(args -> currentRow.get())
                .build()));

        final ChunkProducer<String, Object> chunkProducer
                = new StreamingChunkProducer(table, columnGenerators, COLUMN_INCLUDE_PREDICATE);

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

            chunkProducer.produceChunks((values, rowEstimate) -> {
                writer.write(Chunk.of(values));
                currentRow.incrementAndGet();
                return true;
            });
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private Map<Column, ValueGenerator<?>> createColumnGenerators(Table table, List<FunctionDef> functionDefs) {
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
