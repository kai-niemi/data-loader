package io.roach.volt.shell;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.roach.volt.csv.ModelConfigException;
import io.roach.volt.csv.TableConfigException;
import io.roach.volt.csv.event.AbstractEventPublisher;
import io.roach.volt.csv.event.CancellationEvent;
import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.ExitEvent;
import io.roach.volt.csv.event.GenerateEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.event.ProducerCancelledEvent;
import io.roach.volt.csv.event.ProducerCompletedEvent;
import io.roach.volt.csv.event.ProducerScheduledEvent;
import io.roach.volt.csv.event.ProducerStartedEvent;
import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.csv.model.Root;
import io.roach.volt.csv.model.Table;
import io.roach.volt.csv.producer.AsyncChunkProducers;
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

    private final AtomicBoolean quitOnCompletion = new AtomicBoolean();

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
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) {
        activeProducers.clear();
        if (quitOnCompletion.get()) {
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
            List<AsyncChunkProducers.ProducerFactory> builders = AsyncChunkProducers.options()
                    .stream()
                    .filter(producerFactory -> producerFactory.test(table))
                    .toList();

            builders.forEach(builder -> builder.validate(table));

            if (builders.isEmpty()) {
                throw new TableConfigException("No suitable chunk producer - ambiguous column refs and/or row counts",
                        table);
            }

            if (builders.size() > 1) {
                throw new TableConfigException("Ambiguous table configuration", table);
            }
        }

        if (applicationModel.getTables().isEmpty()) {
            console.red("No tables found in current model. "
                    + "Use the schema export command 'db-export' or edit the application model YAML file directly. %s"
                    .formatted(AsciiArt.shrug())).nl();
        }
    }

    @ShellMethodAvailability("ifActiveProducers")
    @ShellMethod(value = "Cancel all background operations", key = {"csv-cancel", "c"})
    public void cancel() {
        publishEvent(new CancellationEvent());
    }

    @ShellMethodAvailability("ifNoActiveProducers")
    @ShellMethod(value = "Generate CSV files from application model", key = {"csv-generate", "g"})
    public void generate(@ShellOption(help = "quit on completion", defaultValue = "false") boolean quit,
                         @ShellOption(help = "disallow base path creation", defaultValue = "false") boolean skipCreateBasePath,
                         @ShellOption(help = "file name prefix", defaultValue = "") String prefix,
                         @ShellOption(help = "file name suffix", defaultValue = ".csv") String suffix) {
        validateModel();

        final Path basePath = Paths.get(applicationModel.getOutputPath());

        if (!Files.isDirectory(basePath)) {
            try {
                if (skipCreateBasePath) {
                    throw new ModelConfigException("Base path not found (or not a directory): " + basePath);
                }
                Set<PosixFilePermission> permissions
                        = PosixFilePermissions.fromString("drwx-r-xr-x"); // 755
                FileAttribute<Set<PosixFilePermission>> fileAttributes
                        = PosixFilePermissions.asFileAttribute(permissions);
                Files.createDirectories(basePath, fileAttributes);
                logger.info("Created base path '%s' with permissions [%s]".formatted(basePath, fileAttributes));
            } catch (IOException e) {
                throw new UncheckedIOException("Base path could not be created: " + basePath, e);
            }
        }

        if (!Files.isWritable(basePath)) {
            throw new ModelConfigException("Base path is not writable - check permissions: " + basePath);
        }

        publishEvent(new GenerateEvent());

        // Have all producers initialize (setting up proper subscriptions etc) and then wait on a given go signal
        final CountDownLatch startLatch = new CountDownLatch(applicationModel.getTables().size());

        quitOnCompletion.set(quit);

        applicationModel.getTables().forEach(table -> {
            Path path = basePath.resolve("%s%s%s".formatted(prefix, table.getName(), suffix));
            publishEvent(new ProducerScheduledEvent(table, path, startLatch));
        });
    }
}
