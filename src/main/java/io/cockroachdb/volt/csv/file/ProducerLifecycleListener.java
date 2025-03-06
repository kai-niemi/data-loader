package io.cockroachdb.volt.csv.file;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.cockroachdb.volt.csv.event.AbstractEventPublisher;
import io.cockroachdb.volt.csv.event.ProducersCompletedEvent;
import io.cockroachdb.volt.csv.event.GenericEvent;
import io.cockroachdb.volt.csv.event.ProducerCancelledEvent;
import io.cockroachdb.volt.csv.event.ProducerCompletedEvent;
import io.cockroachdb.volt.csv.event.ProducerFailedEvent;
import io.cockroachdb.volt.csv.event.ProducerProgressEvent;
import io.cockroachdb.volt.csv.event.ProducerStartedEvent;
import io.cockroachdb.volt.csv.model.Table;
import io.cockroachdb.volt.shell.support.AnsiConsole;
import io.cockroachdb.volt.util.ByteUtils;

@Component
public class ProducerLifecycleListener extends AbstractEventPublisher {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<Table> activeTables = Collections.synchronizedList(new ArrayList<>());

    @Autowired
    private AnsiConsole console;

    @EventListener
    public void onStartedEvent(GenericEvent<ProducerStartedEvent> event) {
        ProducerStartedEvent startedEvent = event.getTarget();

        activeTables.add(startedEvent.getTable());

        if (startedEvent.isBoundedCount()) {
            logger.info("Started generating '%s': %,d rows".formatted(
                    startedEvent.getPath(),
                    startedEvent.getTable().getFinalCount()));
        } else {
            logger.info("Started generating '%s': ∞ rows using".formatted(
                    startedEvent.getPath()));
        }
    }

    @EventListener
    public void onProgressEvent(GenericEvent<ProducerProgressEvent> event) {
        ProducerProgressEvent pe = event.getTarget();
        if (pe.getTotal() > 0) {
            console.progressBar(
                    pe.getPosition(),
                    pe.getTotal(),
                    pe.getLabel(),
                    pe.getRequestsPerSec(),
                    pe.remainingMillis());
        }
    }

    @EventListener
    public void onCompletedEvent(GenericEvent<ProducerCompletedEvent> event) {
        activeTables.remove(event.getTarget().getTable());

        logger.info("Completed generating '%s': %,d rows in %s (%.0f/s avg) (%s) - %d in queue".formatted(
                event.getTarget().getPath(),
                event.getTarget().getRows(),
                event.getTarget().getDuration(),
                event.getTarget().calcRequestsPerSec(),
                ByteUtils.byteCountToDisplaySize(event.getTarget().getPath().toFile().length()),
                activeTables.size())
        );
    }

    @EventListener
    public void onCancelledEvent(GenericEvent<ProducerCancelledEvent> event) {
        logger.warn("Cancelled generating CSV for table '%s'"
                .formatted(event.getTarget().getTable()));
    }

    @EventListener
    public void onFailedEvent(GenericEvent<ProducerFailedEvent> event) {
        logger.error("Failed to generating CSV for table '%s'"
                        .formatted(event.getTarget().getTable().getName()),
                event.getTarget().getCause());
    }

    @EventListener
    public void onCompletionEvent(GenericEvent<ProducersCompletedEvent> event) {
        logger.info("Successfully produced all CSV file(s)");
    }
}
