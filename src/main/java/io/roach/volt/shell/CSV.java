package io.roach.volt.shell;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.data.util.Pair;
import org.springframework.scheduling.annotation.Async;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.roach.volt.csv.ConfigurationException;
import io.roach.volt.csv.event.AbstractEventPublisher;
import io.roach.volt.csv.event.CancellationEvent;
import io.roach.volt.csv.event.ProducersCompletedEvent;
import io.roach.volt.csv.event.ExitEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.event.ProducerCancelledEvent;
import io.roach.volt.csv.event.ProducerCompletedEvent;
import io.roach.volt.csv.event.ProducerFailedEvent;
import io.roach.volt.csv.event.ProducerStartedEvent;
import io.roach.volt.csv.event.ProducersStartingEvent;
import io.roach.volt.csv.file.ChunkProducerQualifier;
import io.roach.volt.csv.file.ChunkProducers;
import io.roach.volt.csv.file.CsvFileProducer;
import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.csv.model.ImportInto;
import io.roach.volt.csv.model.Root;
import io.roach.volt.csv.model.Table;
import io.roach.volt.shell.support.AnsiConsole;
import io.roach.volt.util.AsciiArt;

@ShellComponent
@ShellCommandGroup(CommandGroups.GEN)
public class CSV extends AbstractEventPublisher {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private AnsiConsole console;

    @Autowired
    private ApplicationModel applicationModel;

    @Autowired
    @Qualifier("yamlObjectMapper")
    private ObjectMapper yamlObjectMapper;

    @Autowired
    private CsvFileProducer csvFileProducer;

    private final List<Path> activeProducers
            = Collections.synchronizedList(new ArrayList<>());

    public Availability ifNoActiveProducers() {
        return !activeProducers.isEmpty()
                ? Availability.unavailable("There are " + activeProducers.size() + " active producers!")
                : Availability.available();
    }

    public Availability ifActiveProducers() {
        return !activeProducers.isEmpty()
                ? Availability.available()
                : Availability.unavailable("There are no active producers!");
    }

    @EventListener
    public void onStartedEvent(GenericEvent<ProducerStartedEvent> event) {
        activeProducers.add(event.getTarget().getPath());
    }

    @EventListener
    public void onFinishedEvent(GenericEvent<ProducerCompletedEvent> event) {
        activeProducers.remove(event.getTarget().getPath());
    }

    @EventListener
    public void onCancelledEvent(GenericEvent<ProducerCancelledEvent> event) {
        activeProducers.remove(event.getTarget().getPath());
    }

    @EventListener
    public void onFailedEvent(GenericEvent<ProducerFailedEvent> event) {
        activeProducers.remove(event.getTarget().getPath());
    }

    @EventListener
    public void onCompletionEvent(GenericEvent<ProducersCompletedEvent> event) {
        activeProducers.clear();

        ImportInto importInto = applicationModel.getImportInto();
        if (importInto == null && event.getTarget().isQuit()) {
            logger.info("Quit on completion - sending exit event");
            publishEvent(new ExitEvent(0));
        }
    }

    @ShellMethod(value = "Show application model YAML", key = {"csv-show", "s"})
    public void show() throws IOException {
        console.blue(yamlObjectMapper.writerFor(Root.class)
                .writeValueAsString(new Root(applicationModel))).nl();
    }

    @ShellMethod(value = "Validate application model YAML", key = {"csv-validate", "v"})
    public void validate() {
        validateModel();
        console.blue("All good %s".formatted(AsciiArt.happy())).nl();
    }

    private void validateModel() {
        for (Table table : applicationModel.getTables()) {
            // Find suitable chunk producer for table and validate
            List<ChunkProducerQualifier> builders = ChunkProducers.allQualifiers()
                    .stream()
                    .filter(chunkProducerQualifier -> chunkProducerQualifier.test(table))
                    .toList();

            builders.forEach(builder -> builder.validate(applicationModel.getTables(), table));

            if (builders.isEmpty()) {
                throw new ConfigurationException("No suitable chunk producer - ambiguous column refs and/or row counts",
                        table);
            }

            if (builders.size() > 1) {
                throw new ConfigurationException("Ambiguous table configuration", table);
            }
        }

        if (applicationModel.getTables().isEmpty()) {
            console.red("No tables found in current model (bad profile name?) %s."
                    + "Use the schema export command 'db-export' or create an application model YAML file.\n"
                    + "See: https://github.com/cloudneutral/volt/README.md"
                    .formatted(AsciiArt.shrug())).nl();
        }
    }

    @ShellMethod(value = "Cancel any active background operations", key = {"csv-cancel", "c"})
    public void cancel() {
        publishEvent(new CancellationEvent());
    }

    @Async
    @ShellMethodAvailability("ifNoActiveProducers")
    @ShellMethod(value = "Generate CSV files from application model", key = {"csv-generate", "g"})
    public void generate(@ShellOption(help = "quit on completion", defaultValue = "false") boolean quit,
                         @ShellOption(help = "file name prefix", defaultValue = "") String prefix,
                         @ShellOption(help = "file name suffix", defaultValue = ".csv") String suffix) {
        validateModel();

        final Path basePath = Paths.get(applicationModel.getOutputPath());

        if (!Files.isDirectory(basePath)) {
            try {
                Files.createDirectories(basePath);
                logger.info("Created base path '%s'".formatted(basePath));
            } catch (IOException e) {
                throw new UncheckedIOException("Base path could not be created: " + basePath, e);
            }
        }

        if (!Files.isWritable(basePath)) {
            throw new ConfigurationException("Base path is not writable - check permissions: " + basePath);
        }

        publishEvent(new ProducersStartingEvent());

        final CountDownLatch startLatch = new CountDownLatch(applicationModel.getTables().size());
        final List<Task> futures = new ArrayList<>();

        applicationModel.getTables().forEach(table -> {
            Path path = basePath.resolve("%s%s%s".formatted(prefix, table.getName(), suffix));

            Task task = new Task();
            task.table = table;
            task.path = path;
            task.future = csvFileProducer.start(table, path, startLatch);

            futures.add(task);

            publishEvent(new ProducerStartedEvent(table, path));
        });

        while (!futures.isEmpty()) {
            Task t = futures.remove(0);
            try {
                Pair<Integer, Duration> result = t.future.join();

                publishEvent(new ProducerCompletedEvent(t.table, t.path)
                        .setDuration(result.getSecond())
                        .setRows(result.getFirst())
                );
            } catch (CancellationException e) {
                publishEvent(new ProducerCancelledEvent(t.table, t.path));
            } catch (CompletionException e) {
                publishEvent(new ProducerFailedEvent(t.table, t.path)
                        .setCause(e.getCause()));
                logger.warn("Cancelling %d futures".formatted(futures.size()));
                futures.forEach(task -> task.future.cancel(true));
            }
        }

        publishEvent(new ProducersCompletedEvent(quit));
    }

    private static class Task {
        Table table;

        Path path;

        CompletableFuture<Pair<Integer, Duration>> future;
    }
}
