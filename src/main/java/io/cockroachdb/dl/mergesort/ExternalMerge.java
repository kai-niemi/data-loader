package io.cockroachdb.dl.mergesort;

import io.cockroachdb.dl.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ExternalMerge {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final ExternalMerge instance = new ExternalMerge();

        private Builder() {
        }

        public Builder withInputFiles(List<Path> files) {
            instance.inputFiles.addAll(files);
            return this;
        }

        public Builder withOutputFile(Path outputFile) {
            instance.outputFile = outputFile;
            return this;

        }

        public Builder withLinesToSkip(int linesToSkip) {
            instance.linesToSkip = linesToSkip;
            return this;
        }

        public Builder withProgressConsumer(Consumer<Progress> progressConsumer) {
            instance.progressConsumer = progressConsumer;
            return this;
        }

        public ExternalMerge build() {
            return instance;
        }

    }

    private static long calculateLines(Path path) {
        try {
            try (Stream<String> stream = Files.lines(path)) {
                return stream.count();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<Path> inputFiles = new ArrayList<>();

    private Path outputFile;

    private int linesToSkip;

    private Consumer<Progress> progressConsumer = (p) -> {
    };

    private Instant lastTime = Instant.now();

    private final Consumer<Progress> progressConsumerThrottle = (p) -> {
        if (Duration.between(lastTime, Instant.now()).toMillis() > 1000) {
            progressConsumer.accept(p);
            lastTime = Instant.now();
        }
    };

    private ExternalMerge() {
    }

    public void merge() {
        Assert.notEmpty(inputFiles, "input files missing");
        Assert.notNull(outputFile, "outputFile is null");

        final Instant start = Instant.now();

        logger.info("Calculating lines in %d files".formatted(inputFiles.size()));

        long totalLines = inputFiles.stream()
                .mapToLong(ExternalMerge::calculateLines).sum();

        logger.info("Merging %d files of %s into %s ".formatted(inputFiles.size(),
                ByteUtils.byteCountToDisplaySize(totalLines), outputFile));

        final AtomicInteger lines = new AtomicInteger();

        inputFiles
                .stream()
                .sorted()
                .forEach(path -> {
                    try (BufferedWriter writer = Files.newBufferedWriter(outputFile,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                        try (Stream<String> stream = Files.lines(path)
                                .skip(linesToSkip)
                                .sequential()) {
                            stream.forEach(line -> {
                                try {
                                    writer.write(line);
                                    writer.newLine();

                                    progressConsumerThrottle.accept(Progress.builder()
                                            .withCurrent(lines.incrementAndGet())
                                            .withTotal(totalLines)
                                            .withNote(path.getFileName().toString())
                                            .build());
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        logger.info("Merged %s into %s in %s"
                .formatted(inputFiles, outputFile, Duration.between(start, Instant.now())));
    }
}

