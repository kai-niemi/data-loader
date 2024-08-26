package io.roach.volt.csv.listener;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.ExitEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.event.ProduceStartEvent;
import io.roach.volt.csv.event.ProducerFailedEvent;
import io.roach.volt.csv.event.ProducerFinishedEvent;
import io.roach.volt.csv.event.ProducerProgressEvent;
import io.roach.volt.csv.event.ProducerStartedEvent;
import io.roach.volt.csv.model.Table;
import io.roach.volt.util.ByteUtils;

@Component
public class LifeCycleListener extends AbstractEventPublisher {
    private final List<Table> activeTables = Collections.synchronizedList(new ArrayList<>());

    private final AtomicBoolean completed = new AtomicBoolean();

    private final AtomicBoolean quitOnCompletion = new AtomicBoolean();

    private final Map<Table, List<Path>> paths = new LinkedHashMap<>();

    @EventListener
    public void onStartEvent(GenericEvent<ProduceStartEvent> event) {
        quitOnCompletion.set(event.getTarget().isQuitOnCompletion());
        completed.set(false);
    }

    @EventListener
    public void onStartedEvent(GenericEvent<ProducerStartedEvent> event) {
        activeTables.add(event.getTarget().getTable());

        if (event.getTarget().isBounded()) {
            console.blue("Started generating '%s' with %,d n:of rows".formatted(
                            event.getTarget().getPath().getFileName(),
                            event.getTarget().getTable().getFinalCount()))
                    .nl();
        } else {
            console.blue("Started generating '%s' with inferred n:of rows".formatted(
                            event.getTarget().getPath().getFileName()))
                    .nl();
        }
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
            console.blue("Cancelled generating '%s' with %,d rows in %s (%.0f/s avg) - %d file(s) in queue"
                            .formatted(
                                    event.getTarget().getPath().getFileName(),
                                    event.getTarget().getRows(),
                                    event.getTarget().getDuration(),
                                    event.getTarget().calcRequestsPerSec(),
                                    activeTables.size()))
                    .nl();
        } else {
            console.blue("Finished generating '%s' with size %s and %,d rows in %s (%.0f/s avg) - %d file(s) in queue"
                            .formatted(
                                    event.getTarget().getPath().getFileName(),
                                    ByteUtils.byteCountToDisplaySize(
                                            event.getTarget().getPath().toFile().length()),
                                    event.getTarget().getRows(),
                                    event.getTarget().getDuration(),
                                    event.getTarget().calcRequestsPerSec(),
                                    activeTables.size()))
                    .nl();
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
        console.blue("Failed generating '%s': %s"
                        .formatted(event.getTarget().getTable(), event.getTarget().getCause()))
                .nl();
    }
}
