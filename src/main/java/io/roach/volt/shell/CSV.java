package io.roach.volt.shell;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.roach.volt.csv.TableConfigException;
import io.roach.volt.csv.event.CancellationEvent;
import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.event.ProduceStartEvent;
import io.roach.volt.csv.event.ProducerFinishedEvent;
import io.roach.volt.csv.event.ProducerStartedEvent;
import io.roach.volt.csv.listener.AbstractEventPublisher;
import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.csv.model.Root;
import io.roach.volt.csv.model.Table;
import io.roach.volt.csv.producer.ChunkProducers;
import io.roach.volt.shell.support.AnsiConsole;
import io.roach.volt.util.Multiplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

@ShellComponent
@ShellCommandGroup(CommandGroups.GEN)
public class CSV extends AbstractEventPublisher {
    @Autowired
    private ApplicationModel applicationModel;

    @Autowired
    @Qualifier("yamlObjectMapper")
    private ObjectMapper yamlObjectMapper;

    @Autowired
    private AnsiConsole console;

    private final List<Path> activeProducers = Collections.synchronizedList(new ArrayList<>());

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
    public void onFinishedEvent(GenericEvent<ProducerFinishedEvent> event) {
        activeProducers.remove(event.getTarget().getPath());
    }

    @EventListener
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) {
        activeProducers.clear();
    }

    @ShellMethodAvailability("ifActiveProducers")
    @ShellMethod(value = "Cancel all background operations", key = {"csv-cancel", "c"})
    public void cancel() {
        publishEvent(new CancellationEvent());
    }

    @ShellMethod(value = "Show application model YAML", key = {"csv-show", "cs"})
    public void show() throws IOException {
        console.blue(yamlObjectMapper.writerFor(Root.class)
                .writeValueAsString(new Root(applicationModel))).nl();
    }

    @ShellMethod(value = "Validate application model YAML", key = {"csv-validate", "cv"})
    public void validate() {
        validateModel();
        console.blue("All good").nl();
    }

    private void validateModel() {
        double total = 0;

        for (Table table : applicationModel.getTables()) {
            int count = 0;

            if (StringUtils.hasLength(table.getCount())) {
                if (table.getCount().contains("%")) {
                    double percentage = Double.parseDouble(table.getCount().replace("%", ""));
                    if (percentage < 0 || percentage > 100) {
                        throw new TableConfigException("Percentage must be 0 >= N <= 100", table);
                    }
                    total += percentage;
                    count = (int) (Multiplier.parseDouble(applicationModel.getCount()) * percentage / 100.0);
                } else {
                    count = (int) Multiplier.parseDouble(table.getCount());
                }
            }

            table.setFinalCount(count);

            // Find suitable chunk producer for table and validate
            List<ChunkProducers.ProducerBuilder> builders = ChunkProducers.options()
                    .stream()
                    .filter(producerBuilder -> producerBuilder.test(table))
                    .toList();

            builders.forEach(builder -> builder.validate(table));

            if (builders.isEmpty()) {
                throw new TableConfigException("No suitable chunk producer - ambiguous column refs and/or row counts", table);
            }

            if (builders.size() > 1) {
                throw new TableConfigException("Ambiguous table configuration", table);
            }
        }

        if (applicationModel.getTables().isEmpty()) {
            logger.warn("No tables found in current model. Use the schema export command 'db-export' or edit the application model YAML file directly.");
        }

        if (total > 0 && total != 100.0) {
            logger.warn("Table row distribution does not add up to 100% but " + total);
        }
    }

    @ShellMethodAvailability("ifNoActiveProducers")
    @ShellMethod(value = "Generate CSV files from application model", key = {"csv-generate", "g"})
    public void generate(@ShellOption(help = "quit on completion", defaultValue = "false") boolean quit,
                         @ShellOption(help = "file name suffix", defaultValue = "") String suffix) {
        validateModel();

        final Path basePath = Paths.get(applicationModel.getOutputPath());

        if (!Files.isDirectory(basePath)) {
            try {
                logger.trace("Create output base path: %s".formatted(basePath));
                Files.createDirectories(basePath);
            } catch (IOException e) {
                throw new UncheckedIOException("Base path could not be created: " + basePath, e);
            }
        }

        if (!Files.isWritable(basePath)) {
            throw new RuntimeException("Base path is not writable: " + basePath);
        }

        for (Table table : applicationModel.getTables()) {
            final int files = table.getFiles();

            table.setFinalCount(table.getFinalCount() / files);

            IntStream.rangeClosed(1, files).forEach(value -> {
                Path path = basePath.resolve(files > 1
                        ? "%s%s-%03d.csv".formatted(table.getName(), suffix, value)
                        : "%s%s.csv".formatted(table.getName(), suffix)
                );

                publishEvent(new ProduceStartEvent(table, path, quit));
            });
        }
    }
}
