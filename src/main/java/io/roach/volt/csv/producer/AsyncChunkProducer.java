package io.roach.volt.csv.producer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import io.roach.volt.csv.generator.ValueGenerator;
import io.roach.volt.csv.generator.ValueGenerators;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Table;
import io.roach.volt.expression.ExpressionRegistry;
import io.roach.volt.expression.ExpressionRegistryBuilder;
import io.roach.volt.expression.FunctionDef;
import io.roach.volt.util.concurrent.CircularFifoQueue;
import io.roach.volt.util.concurrent.FifoQueue;
import io.roach.volt.util.pubsub.Publisher;

public abstract class AsyncChunkProducer implements ChunkProducer<String, Object> {
    protected static final Predicate<Column> COLUMN_INCLUDE_PREDICATE
            = column -> (column.isHidden() == null || !column.isHidden());

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final Map<Column, ValueGenerator<?>> columnGenerators = new LinkedHashMap<>();

    protected final FifoQueue<String, Object> fifoQueue = new CircularFifoQueue<>(8196);

    protected DataSource dataSource;

    protected Publisher publisher;

    protected Table table;

    protected AtomicInteger currentRow;

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setCurrentRow(AtomicInteger currentRow) {
        this.currentRow = currentRow;
    }

    public final void initialize() {
        Assert.notNull(dataSource, "dataSource is null");
        Assert.notNull(publisher, "publisher is null");
        Assert.notNull(table, "table is null");
        Assert.notNull(currentRow, "currentRow is null");

        ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);

        List.of(FunctionDef.builder()
                        .withCategory("other")
                        .withId("rowNumber")
                        .withDescription("Returns current row number in CSV file.")
                        .withReturnValue(Integer.class)
                        .withFunction(args -> currentRow.get())
                        .build())
                .forEach(registry::addFunction);

        // Get generator for all non-ref columns
        table.filterColumns(Table.WITH_REF
                        .or(Table.WITH_EACH).negate())
                .forEach(column -> columnGenerators.put(column,
                        ValueGenerators.createValueGenerator(column, dataSource, registry)));

        doInitialize();
    }

    protected void doInitialize() {
    }

    protected Map<String, Object> filterIncludes(Map<String, Object> map) {
        Map<String, Object> copy = new LinkedHashMap<>();

        table.getColumns()
                .stream()
                .filter(COLUMN_INCLUDE_PREDICATE)
                .map(Column::getName)
                .forEach(column -> copy.put(column, map.get(column)));

        return copy;
    }
}