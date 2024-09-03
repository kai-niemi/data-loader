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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import io.roach.volt.csv.event.AbstractEventPublisher;
import io.roach.volt.csv.event.CancellationEvent;
import io.roach.volt.csv.event.GenerateEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.event.ProducerCancelledEvent;
import io.roach.volt.csv.event.ProducerCompletedEvent;
import io.roach.volt.csv.event.ProducerFailedEvent;
import io.roach.volt.csv.event.ProducerProgressEvent;
import io.roach.volt.csv.event.ProducerScheduledEvent;
import io.roach.volt.csv.event.ProducerStartedEvent;
import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.ImportOption;
import io.roach.volt.csv.model.Table;
import io.roach.volt.csv.producer.AsyncChunkProducer;
import io.roach.volt.csv.producer.AsyncChunkProducers;
import io.roach.volt.csv.stream.CsvStreamWriter;
import io.roach.volt.csv.stream.CsvStreamWriterBuilder;
import io.roach.volt.pubsub.Publisher;

@Component
public class CsvFileProducer extends AbstractEventPublisher {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final AtomicBoolean cancellationRequested = new AtomicBoolean();

    @Autowired
    private ApplicationModel applicationModel;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private Publisher publisher;

    @EventListener
    public void onCancellationEvent(GenericEvent<CancellationEvent> event) {
        logger.info("Cancellation request received");
        cancellationRequested.set(true);
    }

    @EventListener
    public void onGenerateEvent(GenericEvent<GenerateEvent> event) {
        cancellationRequested.set(false);
    }

    @Async
    @EventListener
    public CompletableFuture<Integer> onScheduledEvent(GenericEvent<ProducerScheduledEvent> event) {
        if (cancellationRequested.get()) {
            logger.warn("Cancellation has been requested - skipping");
            return CompletableFuture.completedFuture(0);
        }

        final ProducerScheduledEvent scheduledEvent = event.getTarget();
        final Path path = scheduledEvent.getPath();
        final Table table = scheduledEvent.getTable();
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

        // Allow all producers to initialize before any starts producing (via latch)
        chunkProducer.initialize();

        try {
            CountDownLatch latch = scheduledEvent.getStartLatch();
            latch.countDown();
            logger.info("Counting down start latch - remaining %d".formatted(latch.getCount()));
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        publishEvent(new ProducerStartedEvent(table, path, chunkProducer.getClass().getSimpleName()));

        try (CsvStreamWriter<Map<String, Object>> writer = createCsvStreamWriter(table, path)) {
            chunkProducer.produceChunks((values, rowEstimate) -> {
                if (cancellationRequested.get()) {
                    logger.warn("Cancellation has been requested - aborting prematurely");
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
            if (failCause != null) {
                publishEvent(new ProducerFailedEvent(table, path)
                        .setCause(failCause)
                );
            } else if (cancellationRequested.get()) {
                publishEvent(new ProducerCancelledEvent(table, path));
            } else {
                publishEvent(new ProducerCompletedEvent(table, path)
                        .setDuration(Duration.between(startTime, Instant.now()))
                        .setRows(currentRow.get())
                );
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
