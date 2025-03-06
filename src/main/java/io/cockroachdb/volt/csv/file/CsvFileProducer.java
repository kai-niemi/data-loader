package io.cockroachdb.volt.csv.file;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.data.util.Pair;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import io.cockroachdb.volt.csv.ConfigurationException;
import io.cockroachdb.volt.csv.event.AbstractEventPublisher;
import io.cockroachdb.volt.csv.event.CancellationEvent;
import io.cockroachdb.volt.csv.event.GenericEvent;
import io.cockroachdb.volt.csv.event.ProducerProgressEvent;
import io.cockroachdb.volt.csv.event.ProducersStartingEvent;
import io.cockroachdb.volt.csv.model.ApplicationModel;
import io.cockroachdb.volt.csv.model.Column;
import io.cockroachdb.volt.csv.model.ImportOption;
import io.cockroachdb.volt.csv.model.Table;
import io.cockroachdb.volt.csv.stream.CsvStreamWriter;
import io.cockroachdb.volt.csv.stream.CsvStreamWriterBuilder;
import io.cockroachdb.volt.pubsub.Publisher;

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
        cancellationRequested.set(true);
    }

    @EventListener
    public void onGenerateEvent(GenericEvent<ProducersStartingEvent> event) {
        cancellationRequested.set(false);
    }

    @Async
    public CompletableFuture<Pair<Integer, Duration>> start(Table table, Path path, CountDownLatch startLatch) {
        if (cancellationRequested.get()) {
            logger.warn("Cancellation requested - skipping");
            return CompletableFuture.completedFuture(Pair.of(0, Duration.ofSeconds(0)));
        }

        final ChunkProducerQualifier chunkProducerQualifier =
                ChunkProducers
                        .allQualifiers()
                        .stream()
                        .filter(producerBuilder -> producerBuilder.test(table))
                        .findFirst()
                        .orElseThrow(() -> new ConfigurationException("No suitable chunk producer", table));

        logger.info("Initializing '%s' for table '%s'"
                .formatted(chunkProducerQualifier.description(), table.getName()));

        final ChunkProducer<String, Object> chunkProducer = chunkProducerQualifier.get();
        if (chunkProducer instanceof AsyncProducer producer) {
            // Allow all producers to initialize before any starts producing (via latch)
            producer.initialize(dataSource, publisher, table);
        } else {
            throw new IllegalStateException("Expected async producer, got: "
                    + chunkProducer.getClass().getName());
        }

        final Instant startTime = Instant.now();
        final AtomicReference<Instant> lastTick = new AtomicReference<>(startTime);

        try {
            startLatch.countDown();

            logger.debug("Counting down latch - remaining %d".formatted(startLatch.getCount()));

            startLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }

        try (CsvStreamWriter<Map<String, Object>> writer = createCsvStreamWriter(table, path)) {
            Supplier<Integer> currentRow = chunkProducer.currentRow();

            chunkProducer.produceChunks((values, rowEstimate) -> {
                if (cancellationRequested.get()) {
                    logger.warn("Cancellation requested - aborting prematurely");
                    return false;
                }

                writer.write(Chunk.of(values));

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

            return CompletableFuture.completedFuture(
                    Pair.of(currentRow.get(), Duration.between(startTime, Instant.now()))
            );
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CsvStreamWriter<Map<String, Object>> createCsvStreamWriter(Table table, Path path)
            throws IOException {
        String delimiter = table.getOptions()
                .getOrDefault(ImportOption.delimiter, applicationModel.getOptions()
                        .getOrDefault(ImportOption.delimiter, ","));

        String quoteCharacter = table.getOptions()
                .getOrDefault(ImportOption.fields_enclosed_by, applicationModel.getOptions()
                        .getOrDefault(ImportOption.fields_enclosed_by, ""));
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
