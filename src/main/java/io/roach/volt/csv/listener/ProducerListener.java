package io.roach.volt.csv.listener;

import io.roach.volt.csv.ModelConfigException;
import io.roach.volt.csv.event.CancellationEvent;
import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.event.ProduceStartEvent;
import io.roach.volt.csv.event.ProducerFailedEvent;
import io.roach.volt.csv.event.ProducerFinishedEvent;
import io.roach.volt.csv.event.ProducerProgressEvent;
import io.roach.volt.csv.event.ProducerStartedEvent;
import io.roach.volt.csv.generator.ColumnGenerator;
import io.roach.volt.csv.generator.ColumnGeneratorBuilder;
import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.ImportOption;
import io.roach.volt.csv.model.Table;
import io.roach.volt.csv.producer.ChunkFileWriter;
import io.roach.volt.csv.producer.ChunkProducer;
import io.roach.volt.csv.producer.ChunkProducers;
import io.roach.volt.csv.producer.ChunkWriter;
import io.roach.volt.expression.ExpressionRegistry;
import io.roach.volt.expression.ExpressionRegistryBuilder;
import io.roach.volt.expression.FunctionDef;
import io.roach.volt.util.concurrent.BlockingHashMap;
import io.roach.volt.util.pubsub.Publisher;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class ProducerListener extends AbstractEventPublisher {
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();

    @Autowired
    private ColumnGeneratorBuilder columnGeneratorBuilder;

    @Autowired
    private ApplicationModel applicationModel;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private Publisher publisher;

    @Value("${application.queue-size}")
    private int queueSize;

    @EventListener
    public void onCancelEvent(GenericEvent<CancellationEvent> event) {
        cancellationRequested.set(true);
        console.printf("Cancellation request received").nl();
    }

    @EventListener
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) {
        cancellationRequested.set(false);
    }

    @EventListener
    public CompletableFuture<Integer> onStartEvent(GenericEvent<ProduceStartEvent> event) {
        final Table table = event.getTarget().getTable();
        final Path path = event.getTarget().getPath();

        logger.info("Started producing '%s'".formatted(path));

        final AtomicInteger currentRow = new AtomicInteger();

        // Context specific functions
        FunctionDef f1 = FunctionDef.builder()
                .withCategory("other")
                .withId("rowNumber")
                .withDescription("Returns current row number in CSV file.")
                .withReturnValue(Integer.class)
                .withFunction(args -> currentRow.get())
                .build();

        final ChunkWriter<Map<String, Object>> csvWriter = createWriter(path);

        Exception failCause = null;
        try {
            csvWriter.open(table.getColumns());

            final Map<Column, ColumnGenerator<?>> generatorMap = createColumnGenerators(table, List.of(f1));

            final ChunkProducer<String, Object> producer =
                    ChunkProducers.options()
                            .stream()
                            .filter(producerBuilder -> producerBuilder.test(table))
                            .findFirst()
                            .orElseThrow(() -> new ModelConfigException("No suitable chunk producer for table configuration: " + table))
                            .createChunkProducer(table, generatorMap, queueSize);

            publishEvent(new ProducerStartedEvent(table, path));

            final Instant startTime = Instant.now();

            final AtomicReference<Instant> lastTick = new AtomicReference<>(Instant.now());

            producer.produce(publisher, (values, rowEstimate) -> {
                if (cancellationRequested.get()) {
                    console.printf("Cancelling producer for '%s' at %,d of %,d rows"
                            .formatted(path.getFileName().toString(),
                                    currentRow.get(),
                                    rowEstimate)).nl();
                    return false;
                }

                Map<String, Object> copy = new LinkedHashMap<>(values);

                table.getColumns().stream()
                        .filter(column -> column.isHidden() != null ? column.isHidden() : false)
                        .forEach(column -> copy.remove(column.getName()));

                csvWriter.writeChunk(Chunk.of(copy));

                currentRow.incrementAndGet();

                if (rowEstimate > 0 && Duration.between(lastTick.get(), Instant.now()).getSeconds() > 1.0) {
                    publishEvent(new ProducerProgressEvent(table, path)
                            .setPosition(currentRow.get())
                            .setTotal(rowEstimate)
                            .setStartTime(startTime)
                            .setLabel("Writing %s".formatted(path.toString()))
                    );
                    lastTick.set(Instant.now());
                }

                return true;
            });

            publishEvent(new ProducerFinishedEvent(table, path)
                    .setDuration(Duration.between(startTime, Instant.now()))
                    .setRows(currentRow.get())
                    .setCancelled(cancellationRequested.get())
            );

            return CompletableFuture.completedFuture(currentRow.get());
        } catch (Exception e) {
            failCause = e;
            return CompletableFuture.failedFuture(e);
        } finally {
            csvWriter.close();

            if (failCause != null) {
                publishEvent(new ProducerFailedEvent(table, path)
                        .setCause(failCause)
                );
            }
        }
    }

    private Map<Column, ColumnGenerator<?>> createColumnGenerators(Table table, List<FunctionDef> functionDefs) {
        ExpressionRegistry registry = ExpressionRegistryBuilder.build(dataSource);
        functionDefs.forEach(registry::addFunction);

        Map<Column, ColumnGenerator<?>> generatorMap = new HashMap<>();

        // Get generator for all non-ref columns
        table.filterColumns(Table.WITH_REF.or(Table.WITH_EACH).negate())
                .forEach(column -> generatorMap.put(column,
                        columnGeneratorBuilder.createColumnGenerator(column, registry)));

        return generatorMap;
    }

    private ChunkWriter<Map<String, Object>> createWriter(Path path) {
        String delimiter = applicationModel.getImport().getOptions().getOrDefault(ImportOption.delimiter, ",");
        String quoteChar = applicationModel.getImport().getOptions().getOrDefault(ImportOption.fields_enclosed_by, "");
        boolean append = applicationModel.isAppend() != null ? applicationModel.isAppend() : false;
        return new ChunkFileWriter<>(
                delimiter,
                quoteChar,
                append,
                path);
    }
}
