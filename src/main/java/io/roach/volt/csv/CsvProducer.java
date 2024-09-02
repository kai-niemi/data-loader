package io.roach.volt.csv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import io.roach.volt.csv.event.AbstractEventPublisher;
import io.roach.volt.csv.event.CancellationEvent;
import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.event.ProducerFailedEvent;
import io.roach.volt.csv.event.ProducerFinishedEvent;
import io.roach.volt.csv.event.ProducerProgressEvent;
import io.roach.volt.csv.event.ProducerStartEvent;
import io.roach.volt.csv.event.ProducerStartedEvent;
import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.ImportOption;
import io.roach.volt.csv.model.Table;
import io.roach.volt.csv.producer.AsyncChunkProducer;
import io.roach.volt.csv.producer.AsyncChunkProducers;
import io.roach.volt.csv.stream.CsvStreamWriter;
import io.roach.volt.csv.stream.CsvStreamWriterBuilder;
import io.roach.volt.util.pubsub.Publisher;

@Component
public class CsvProducer extends AbstractEventPublisher {
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();

    @Autowired
    private ApplicationModel applicationModel;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private Publisher publisher;

    @EventListener
    public void onCancelEvent(GenericEvent<CancellationEvent> event) {
        cancellationRequested.set(true);
    }

    @EventListener
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) {
        cancellationRequested.set(false);
    }

    @Async
    @EventListener
    public CompletableFuture<Integer> onStartEvent(GenericEvent<ProducerStartEvent> event) {
        if (cancellationRequested.get()) {
            return CompletableFuture.completedFuture(0);
        }

        final Path path = event.getTarget().getPath();
        final Table table = event.getTarget().getTable();
        final AtomicInteger currentRow = new AtomicInteger();
        final Instant startTime = Instant.now();
        final AtomicReference<Instant> lastTick = new AtomicReference<>(startTime);
        Exception failCause = null;

        final AsyncChunkProducer chunkProducer =
                AsyncChunkProducers
                        .options()
                        .stream()
                        .filter(producerBuilder -> producerBuilder.test(table))
                        .findFirst()
                        .orElseThrow(() -> new ModelConfigException(
                                "No suitable chunk producer for table configuration: " + table))
                        .get();
        chunkProducer.setDataSource(dataSource);
        chunkProducer.setPublisher(publisher);
        chunkProducer.setCurrentRow(currentRow);
        chunkProducer.setTable(table);
        chunkProducer.initialize();

        try (CsvStreamWriter<Map<String, Object>> writer = createCsvStreamWriter(table, path)) {
            publishEvent(new ProducerStartedEvent(table, path, chunkProducer.getClass().getSimpleName()));

            chunkProducer.produceChunks((values, rowEstimate) -> {
                if (cancellationRequested.get()) {
                    return false;
                }

                writer.write(Chunk.of(values));

                currentRow.incrementAndGet();

                if (rowEstimate > 0 && Duration.between(lastTick.get(), Instant.now()).getSeconds() > 1.0) {
                    publishEvent(new ProducerProgressEvent(table, path)
                            .setPosition(currentRow.get())
                            .setTotal(rowEstimate)
                            .setStartTime(startTime)
                            .setLabel(path.toString())
                    );
                    lastTick.set(Instant.now());
                }

                return true;
            });

            writer.close();

            return CompletableFuture.completedFuture(currentRow.get());
        } catch (Exception e) {
            failCause = e;
            return CompletableFuture.failedFuture(e);
        } finally {
            publishEvent(new ProducerFinishedEvent(table, path)
                    .setDuration(Duration.between(startTime, Instant.now()))
                    .setRows(currentRow.get())
                    .setCancelled(cancellationRequested.get())
            );

            if (failCause != null) {
                publishEvent(new ProducerFailedEvent(table, path).setCause(failCause));
            }
        }
    }

    private CsvStreamWriter<Map<String, Object>> createCsvStreamWriter(Table table, Path path)
            throws IOException {
        String delimiter = table.getOptions()
                .getOrDefault(ImportOption.delimiter, applicationModel.getImportInto()
                        .getOptions().getOrDefault(ImportOption.delimiter, ","));

        String quoteCharacter = table.getOptions()
                .getOrDefault(ImportOption.fields_enclosed_by, applicationModel.getImportInto()
                        .getOptions().getOrDefault(ImportOption.fields_enclosed_by, ""));
        quoteCharacter = quoteCharacter.replace("(empty)", "");

        List<String> columnNames = table
                .filterColumns(column -> column.isHidden() == null || !column.isHidden())
                .stream()
                .map(Column::getName)
                .toList();

        CsvStreamWriter<Map<String, Object>> itemWriter = new CsvStreamWriterBuilder<Map<String, Object>>()
                .withDelimiter(delimiter)
                .withQuoteCharacter(quoteCharacter)
                .withColumnNames(columnNames)
                .withIncludeHeader(true)
                .build();

        itemWriter.setWriter(new BufferedWriter(new FileWriter(path.toFile())));
        itemWriter.open(new ExecutionContext());

        return itemWriter;
    }

}
