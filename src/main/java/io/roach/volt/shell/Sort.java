package io.roach.volt.shell;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.EnumValueProvider;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import io.roach.volt.csv.event.AbstractEventPublisher;
import io.roach.volt.mergesort.ComparatorType;
import io.roach.volt.mergesort.ExternalMerge;
import io.roach.volt.mergesort.ExternalMergeSort;
import io.roach.volt.mergesort.ExternalSplit;
import io.roach.volt.shell.support.AnotherFileValueProvider;
import io.roach.volt.shell.support.AnsiConsole;
import io.roach.volt.shell.support.DirectoryValueProvider;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

@ShellComponent
@ShellCommandGroup(CommandGroups.SORT)
public class Sort extends AbstractEventPublisher {
    @Autowired
    private AnsiConsole console;

    @ShellMethod(value = "Split and merge sort a CSV file in lexicographical order", key = {"sort"})
    public void sort(
            @ShellOption(help = "input file path",
                    valueProvider = AnotherFileValueProvider.class) String inputFile,
            @ShellOption(help = "output file path (derived from input path if omitted)",
                    defaultValue = ShellOption.NULL,
                    valueProvider = AnotherFileValueProvider.class) String outputFile,
            @ShellOption(help = "comma separated list of CSV column(s) to sort by (one-based). If omitted, all columns are included.",
                    defaultValue = ShellOption.NULL) List<Integer> columns,
            @ShellOption(help = "line comparator type",
                    defaultValue = "auto",
                    valueProvider = EnumValueProvider.class) ComparatorType comparatorType,
            @ShellOption(help = "column delimiter", defaultValue = ",") String delimiter,
            @ShellOption(help = "number of chunks or files (-1 denotes number of host vCPU:s)", defaultValue = "-1")
            int chunks,
            @ShellOption(help = "lines to skip from input file denoting header (included in sorted output)", defaultValue = "0")
            int linesToSkip,
            @ShellOption(help = "keep original input file after completion",
                    defaultValue = "false") boolean skipReplace
    ) {
        if (inputFile.equals(outputFile)) {
            throw new CommandException("Input and output files are the same!");
        }

        ExternalMergeSort externalMergeSort = ExternalMergeSort.builder()
                .withChunks(chunks > 0 ? chunks : Runtime.getRuntime().availableProcessors())
                .withLinesToSkip(linesToSkip)
                .withInputFile(Paths.get(inputFile))
                .withOutputFile(outputFile != null ? Paths.get(outputFile) : null)
                .withOrderByColumns(columns != null ? columns : List.of())
                .withComparator(comparatorType)
                .withDelimiter(delimiter)
                .withReplace(!skipReplace)
                .withProgressConsumer((progress) -> {
                    console.progressBar(
                            progress.getCurrent(),
                            progress.getTotal(),
                            progress.getNote());
                })
                .build();

        publishEvent(externalMergeSort);
    }

    @ShellMethod(value = "Split a CSV file in even chunks without sorting", key = {"split"})
    public void split(
            @ShellOption(help = "input file path",
                    defaultValue = ShellOption.NONE,
                    valueProvider = AnotherFileValueProvider.class) String inputFile,
            @ShellOption(help = "number of chunks or files (-1 denotes number of host vCPU:s)", defaultValue = "-1")
            int chunks,
            @ShellOption(help = "lines to skip from input file denoting header (included in sorted output)", defaultValue = "0")
            int linesToSkip,
            @ShellOption(help = "keep original input file after completion",
                    defaultValue = "false") boolean skipDelete
    ) {
        ExternalSplit externalSplit = ExternalSplit.builder()
                .withChunks(chunks > 0 ? chunks : Runtime.getRuntime().availableProcessors())
                .withLinesToSkip(linesToSkip)
                .withInputFile(Paths.get(inputFile))
                .withDelete(!skipDelete)
                .withProgressConsumer((progress) -> {
                    console.progressBar(
                            progress.getCurrent(),
                            progress.getTotal(),
                            progress.getNote());
                })
                .build();

        publishEvent(externalSplit);
    }

    @ShellMethod(value = "Merge a list of CSV files into a single file", key = {"merge"})
    public void merge(
            @ShellOption(help = "input file base dir",
                    defaultValue = ShellOption.NULL,
                    valueProvider = DirectoryValueProvider.class) String baseDir,
            @ShellOption(help = "input file (glob) pattern",
                    defaultValue = "**/*.csv") String pattern,
            @ShellOption(help = "merge output file path") String outputFile,
            @ShellOption(help = "lines to skip from input file(s) denoting a header",
                    defaultValue = "0") int linesToSkip,
            @ShellOption(help = "only list files included",
                    defaultValue = "false") boolean dryRun
    ) throws IOException {

        PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

        List<Path> inputFiles = Files.find(Paths.get(baseDir),
                        99,
                        (p, a) -> pathMatcher.matches(p),
                        FOLLOW_LINKS)
                .collect(Collectors.toList());

        inputFiles.forEach(path -> {
            console.cyan(path.toString());
        });

        if (inputFiles.isEmpty()) {
            console.red("No files found").nl();
            return;
        }

        if (dryRun) {
            return;
        }

        ExternalMerge externalMerge = ExternalMerge.builder()
                .withInputFiles(inputFiles)
                .withOutputFile(Paths.get(outputFile))
                .withLinesToSkip(linesToSkip)
                .withProgressConsumer((progress) -> {
                    console.progressBar(
                            progress.getCurrent(),
                            progress.getTotal(),
                            progress.getNote());
                })
                .build();

        publishEvent(externalMerge);
    }
}
