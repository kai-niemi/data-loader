package io.roach.volt.shell.mergesort;

import io.roach.volt.util.ByteUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * External merge sort utility for splitting, sorting and merging CSV files for the purpose
 * of optimizing CockroachDB IMPORT speed.
 * <p>
 * A merge-sort is a divide and conquer algorithm similar to map-reduce without the distribution and
 * data processing parts. An external merge-sort simple refers to using non-heap storage such as the
 * storage system due to the size of the data to be sorted does not fit into the main memory of
 * the compute unit.
 * <p>
 * See <a href="https://en.wikipedia.org/wiki/External_sorting">External Sorting</a> for more
 * details.
 *
 * @author Kai Niemi
 */
public class ExternalMergeSort {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final ExternalMergeSort instance = new ExternalMergeSort();

        private final List<Integer> orderBy = new ArrayList<>();

        private ComparatorType comparatorType;

        private String delimiter = ",";

        private Builder() {
        }

        public Builder withInputFile(Path inputFile) {
            instance.inputFile = inputFile;
            return this;
        }

        public Builder withOutputFile(Path outputFile) {
            instance.outputFile = outputFile;
            return this;
        }

        public Builder withChunks(int chunks) {
            instance.chunks = chunks;
            return this;
        }

        public Builder withReplace(boolean replace) {
            instance.replace = replace;
            return this;
        }

        public Builder withLinesToSkip(int linesToSkip) {
            instance.linesToSkip = linesToSkip;
            return this;
        }

        public Builder withOrderByColumns(List<Integer> indexes) {
            indexes.forEach(index -> {
                if (this.orderBy.contains(index)) {
                    throw new IllegalArgumentException("Duplicate index: " + index);
                }
                this.orderBy.add(index);
            });
            return this;
        }

        public Builder withOrderByColumns(Integer... indexes) {
            return withOrderByColumns(Arrays.asList(indexes));
        }

        public Builder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder withComparator(ComparatorType comparatorType) {
            this.comparatorType = comparatorType;
            return this;
        }

        public Builder withProgressConsumer(Consumer<Progress> progressConsumer) {
            instance.progressConsumer = progressConsumer;
            return this;
        }

        public ExternalMergeSort build() {
            if (instance.chunks <= 0) {
                throw new IllegalStateException("chunks < 0");
            }

            instance.comparator = comparatorType.comparator(this.delimiter, this.orderBy);

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

    private static long calculateSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class InputFilePart implements Closeable {
        private final Path path;

        private final BufferedReader bufferedReader;

        private String currentLine;

        public InputFilePart(Path path) {
            this.path = path;
            try {
                this.bufferedReader = new BufferedReader(new FileReader(path.toFile()), 8196);
                readNextLine();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public Path getPath() {
            return path;
        }

        @Override
        public void close() {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public boolean isEmpty() {
            return currentLine == null;
        }

        public String peek() {
            if (isEmpty()) {
                return null;
            }
            return currentLine;
        }

        public String pop() throws IOException {
            String line = peek();
            readNextLine();
            return line;
        }

        private void readNextLine() throws IOException {
            this.currentLine = bufferedReader.readLine();
        }
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Path inputFile;

    private Path outputFile;

    private int chunks = Runtime.getRuntime().availableProcessors();

    private int linesToSkip;

    private boolean replace;

    private final List<String> linesSkipped = new ArrayList<>();

    private Comparator<String> comparator;

    private Consumer<Progress> progressConsumer = (p) -> {
    };

    private Instant lastTime = Instant.now();

    private final Consumer<Progress> progressConsumerThrottle = (p) -> {
        if (Duration.between(lastTime, Instant.now()).toMillis() > 1000) {
            progressConsumer.accept(p);
            lastTime = Instant.now();
        }
    };

    private ExternalMergeSort() {
    }

    public void sort() throws IOException {
        Assert.notNull(inputFile, "inputFile is null");

        if (outputFile == null) {
            outputFile = inputFile.resolveSibling("%s-sorted.%s".formatted(
                    FilenameUtils.getBaseName(inputFile.getFileName().toString()),
                    FilenameUtils.getExtension(inputFile.getFileName().toString())
            ));
            logger.info("Resolved output file: %s".formatted(outputFile));
        }

        final Instant start = Instant.now();

        List<Path> parts = new ArrayList<>();
        split(parts::add);
        merge(parts);
        purge(parts);

        if (replace) {
            logger.info("Moving %s => %s".formatted(outputFile, inputFile));
            Files.move(outputFile, inputFile, StandardCopyOption.REPLACE_EXISTING);
        }

        logger.info("Merge-sort of %s completed in %s"
                .formatted(inputFile, Duration.between(start, Instant.now())));
    }

    private void purge(List<Path> parts) {
        parts.forEach(path -> {
            if (!path.toFile().delete()) {
                logger.warn("Unable to delete: %s".formatted(path));
                path.toFile().deleteOnExit();
            } else {
                logger.info("Deleted: %s".formatted(path));
            }
        });
    }

    private void split(Consumer<Path> sortedParts) throws IOException {
        if (linesToSkip > 0) {
            try (Stream<String> stream = Files.lines(inputFile)
                    .limit(linesToSkip)
                    .sequential()) {
                stream.forEach(linesSkipped::add);
            }
        }

        logger.info("Counting lines in %s".formatted(inputFile));
        long totalLines = calculateLines(inputFile);
        logger.info("Total lines in %s is %,d".formatted(inputFile, totalLines));

        long linesPerChunk = totalLines / chunks;
        AtomicLong lines = new AtomicLong();

        List<CompletableFuture<?>> allFutures = new ArrayList<>();

        IntStream.rangeClosed(1, chunks).forEach(chunk -> {
            long startLine = (chunk == 1 ? linesToSkip : 0) + (chunk - 1) * linesPerChunk + 1;
            long endLine = chunk * linesPerChunk;

            Path inputPart = inputFile.resolveSibling("%s-part-%03d.%s".formatted(
                    FilenameUtils.getBaseName(inputFile.getFileName().toString()),
                    chunk,
                    FilenameUtils.getExtension(inputFile.getFileName().toString()))
            );

            logger.info("Split/sort '%s' chunk %d/%d file '%s' for range %d/%d"
                    .formatted(inputFile, chunk, chunks, inputPart, startLine, endLine));

            CompletableFuture<?> future = CompletableFuture.runAsync(() -> {
                final Instant splitStart = Instant.now();

                try (BufferedWriter writer = Files.newBufferedWriter(inputPart)) {
                    try (Stream<String> stream = Files.lines(inputFile)
                            .skip(startLine - 1)
                            .limit(endLine + 1 - startLine)
                            .sequential()
                            .sorted(comparator)) {
                        stream.forEach(line -> {
                            try {
                                writer.write(line);
                                writer.newLine();

                                progressConsumerThrottle.accept(Progress.builder()
                                        .withCurrent(lines.incrementAndGet())
                                        .withTotal(totalLines)
                                        .withNote("split/sort " + inputPart.getFileName().toString())
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
                    sortedParts.accept(inputPart);
                }
            });

            allFutures.add(future);
        });

        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[] {})).join();
    }

    private void merge(List<Path> sortedInputFiles) throws IOException {
        final PriorityQueue<InputFilePart> priorityQueue = new PriorityQueue<>(sortedInputFiles.size(),
                (left, right) -> comparator.compare(left.peek(), right.peek())
        );

        Collections.sort(sortedInputFiles);

        sortedInputFiles.forEach(path -> priorityQueue.add(new InputFilePart(path)));

        long totalSize = sortedInputFiles.stream()
                .mapToLong(ExternalMergeSort::calculateSize).sum();

        logger.info("Merging %d parts of total size %s into %s"
                .formatted(sortedInputFiles.size(),
                        ByteUtils.byteCountToDisplaySize(totalSize),
                        outputFile));

        try (CountingOutputStream outputStream
                     = new CountingOutputStream(new FileOutputStream(outputFile.toFile()));
             BufferedWriter outputWriter
                     = new BufferedWriter(new OutputStreamWriter(outputStream), 8192)) {
            linesSkipped.forEach(line -> {
                try {
                    outputWriter.write(line);
                    outputWriter.newLine();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            while (!priorityQueue.isEmpty()) {
                InputFilePart inputFilePart = priorityQueue.poll();

                outputWriter.write(inputFilePart.pop());
                outputWriter.newLine();

                progressConsumerThrottle.accept(Progress.builder()
                        .withCurrent(outputStream.getByteCount())
                        .withTotal(totalSize)
                        .withNote("merge " + inputFilePart.getPath().getFileName().toString())
                        .withUnit(Progress.Unit.bytes)
                        .build());

                if (!inputFilePart.isEmpty()) {
                    priorityQueue.add(inputFilePart);
                } else {
                    inputFilePart.close();
                }
            }
        } finally {
            priorityQueue.forEach(InputFilePart::close);
        }
    }
}
