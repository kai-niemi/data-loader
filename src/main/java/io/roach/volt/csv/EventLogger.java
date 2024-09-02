package io.roach.volt.csv;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.roach.volt.csv.event.AbstractEventPublisher;
import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.ExitEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.event.ProducerFailedEvent;
import io.roach.volt.csv.event.ProducerFinishedEvent;
import io.roach.volt.csv.event.ProducerProgressEvent;
import io.roach.volt.csv.event.ProducerStartEvent;
import io.roach.volt.csv.event.ProducerStartedEvent;
import io.roach.volt.csv.model.Table;
import io.roach.volt.shell.support.AnsiConsole;
import io.roach.volt.util.ByteUtils;

@Component
public class EventLogger extends AbstractEventPublisher {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<Table> activeTables = Collections.synchronizedList(new ArrayList<>());

    private final AtomicBoolean completed = new AtomicBoolean();

    private final AtomicBoolean quitOnCompletion = new AtomicBoolean();

    private final Map<Table, List<Path>> paths = Collections.synchronizedMap(new LinkedHashMap<>());

    @Autowired
    private AnsiConsole console;

    @EventListener
    public void onStartEvent(GenericEvent<ProducerStartEvent> event) {
        quitOnCompletion.set(event.getTarget().isQuitOnCompletion());
        completed.set(false);
    }

    @EventListener
    public void onStartedEvent(GenericEvent<ProducerStartedEvent> event) {
        ProducerStartedEvent startedEvent = event.getTarget();

        activeTables.add(startedEvent.getTable());

        if (startedEvent.isBounded()) {
            logger.info("Started generating '%s': %,d rows using '%s'".formatted(
                    startedEvent.getPath(),
                    startedEvent.getTable().getFinalCount(),
                    startedEvent.getProducerInfo()));
        } else {
            logger.info("Started generating '%s': ∞ rows using '%s'".formatted(
                    startedEvent.getPath(),
                    startedEvent.getProducerInfo()));
        }
    }

    @EventListener
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) {
        logger.info("Finished generating all %d file(s)"
                .formatted(event.getTarget().getPaths().size()));
    }

    @EventListener
    public void onProgressEvent(GenericEvent<ProducerProgressEvent> event) {
        ProducerProgressEvent progressEvent = event.getTarget();

        if (progressEvent.getTotal() > 0) {
            console.progressBar(
                    progressEvent.getPosition(),
                    progressEvent.getTotal(),
                    progressEvent.getLabel(),
                    progressEvent.getRequestsPerSec(),
                    progressEvent.remainingMillis());
        }
    }

    @EventListener
    public void onFinishedEvent(GenericEvent<ProducerFinishedEvent> event) {
        paths.computeIfAbsent(event.getTarget().getTable(), s -> new ArrayList<>())
                .add(event.getTarget().getPath());

        activeTables.remove(event.getTarget().getTable());

        if (event.getTarget().isCancelled()) {
            logger.warn("Cancelled generating '%s': %,d rows in %s (%.0f/s avg) - %d file(s) in queue"
                    .formatted(
                            event.getTarget().getPath(),
                            event.getTarget().getRows(),
                            event.getTarget().getDuration(),
                            event.getTarget().calcRequestsPerSec(),
                            activeTables.size()));
        } else {
            logger.info("Finished generating '%s': %,d rows in %s (%.0f/s avg) (%s) - %d file(s) in queue"
                    .formatted(
                            event.getTarget().getPath(),
                            event.getTarget().getRows(),
                            event.getTarget().getDuration(),
                            event.getTarget().calcRequestsPerSec(),
                            ByteUtils.byteCountToDisplaySize(
                                    event.getTarget().getPath().toFile().length()),
                            activeTables.size()));
        }

        if (activeTables.isEmpty() && !completed.get()) {
            completed.set(true);

            if (!event.getTarget().isCancelled()) {
                publishEvent(new CompletionEvent(paths));
            }

            paths.clear();

            if (quitOnCompletion.get()) {
                publishEvent(new ExitEvent(0));
            }
        }
    }

    @EventListener
    public void onFailedEvent(GenericEvent<ProducerFailedEvent> event) {
        logger.error("Failed generating '%s': %s"
                .formatted(event.getTarget().getTable(), event.getTarget().getCause()));
    }
}
