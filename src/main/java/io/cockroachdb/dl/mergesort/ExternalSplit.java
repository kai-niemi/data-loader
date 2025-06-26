package io.cockroachdb.dl.mergesort;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ExternalSplit {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final ExternalSplit instance = new ExternalSplit();

        private Builder() {
        }

        public Builder withInputFile(Path inputFile) {
            instance.inputFile = inputFile;
            return this;
        }

        public Builder withChunks(int chunks) {
            instance.chunks = chunks;
            return this;
        }

        public Builder withDelete(boolean delete) {
            instance.delete = delete;
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

        public ExternalSplit build() {
            if (instance.chunks <= 0) {
                throw new IllegalStateException("chunks < 0");
            }


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

    private Path inputFile;

    private int chunks = Runtime.getRuntime().availableProcessors();

    private int linesToSkip;

    private boolean delete;

    private Consumer<Progress> progressConsumer = (p) -> {
    };

    private Instant lastTime = Instant.now();

    private final Consumer<Progress> progressConsumerThrottle = (p) -> {
        if (Duration.between(lastTime, Instant.now()).toMillis() > 1000) {
            progressConsumer.accept(p);
            lastTime = Instant.now();
        }
    };

    private ExternalSplit() {
    }

    public void split() {
        Assert.notNull(inputFile, "inputFile is null");

        final Instant start = Instant.now();

        splitInputFile();

        if (delete) {
            if (!inputFile.toFile().delete()) {
                logger.warn("Unable to delete: %s".formatted(inputFile));
                inputFile.toFile().deleteOnExit();
            } else {
                logger.info("Deleted: %s".formatted(inputFile));
            }
        }

        logger.info("Split of %s completed in %s"
                .formatted(inputFile, Duration.between(start, Instant.now())));
    }

    private void splitInputFile() {
        logger.info("Counting lines in %s".formatted(inputFile));
        long totalLines = calculateLines(inputFile);
        logger.info("Total lines in %s is %,d".formatted(inputFile, totalLines));

        long linesPerChunk = totalLines / chunks;
        AtomicLong lines = new AtomicLong();

        List<CompletableFuture<?>> allFutures = new ArrayList<>();

        IntStream.rangeClosed(1, chunks).forEach(chunk -> {
            long startLine = (chunk == 1 ? linesToSkip : 0) + (chunk - 1) * linesPerChunk + 1;
            long endLine = chunk * linesPerChunk;

            Path inputPart = inputFile.resolveSibling("%s-%03d.%s".formatted(
                    FilenameUtils.getBaseName(inputFile.getFileName().toString()),
                    chunk,
                    FilenameUtils.getExtension(inputFile.getFileName().toString()))
            );

            logger.info("Split '%s' chunk %d/%d file '%s' for range %d/%d"
                    .formatted(inputFile, chunk, chunks, inputPart, startLine, endLine));

            CompletableFuture<?> future = CompletableFuture.runAsync(() -> {
                final Instant splitStart = Instant.now();

                try (BufferedWriter writer = Files.newBufferedWriter(inputPart)) {
                    try (Stream<String> stream = Files.lines(inputFile)
                            .skip(startLine - 1)
                            .limit(endLine + 1 - startLine)
                            .sequential()) {
                        stream.forEach(line -> {
                            try {
                                writer.write(line);
                                writer.newLine();

                                progressConsumerThrottle.accept(Progress.builder()
                                        .withCurrent(lines.incrementAndGet())
                                        .withTotal(totalLines)
                                        .withNote("split " + inputPart.getFileName().toString())
                                        .build());
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    logger.info("Done splitting %s at range %d/%d in %s"
                            .formatted(inputPart, startLine, endLine, Duration.between(splitStart, Instant.now())));
                }
            });

            allFutures.add(future);
        });

        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[]{})).join();
    }
}
