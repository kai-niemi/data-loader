package io.roach.volt.csv.listener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.PropertyPlaceholderHelper;
import org.springframework.util.StringUtils;

import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.ImportSettings;
import io.roach.volt.csv.model.Table;
import io.roach.volt.shell.support.AnsiConsole;
import io.roach.volt.util.Networking;
import io.roach.volt.util.graph.DirectedAcyclicGraph;

/**
 * Completion event listener that generates a topologically ordered import file
 * for issuing IMPORT commands.
 */
@Component
public class ImportFileListener extends AbstractEventPublisher {
    @Autowired
    private ApplicationModel applicationModel;

    @Autowired
    private AnsiConsole ansiConsole;

    @EventListener
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) throws IOException {
        ImportSettings importSettings = ImportSettings.createDefault();

        applicationModel.setImport(importSettings);

        Map<Table, List<Path>> paths = event.getTarget().getPaths();

        sortByTopologyOrder(paths);

        createImportIntoFile(paths, importSettings);
    }

    private void sortByTopologyOrder(Map<Table, List<Path>> paths) {
        final DirectedAcyclicGraph<Table, String> directedAcyclicGraph = new DirectedAcyclicGraph<>();

        paths.keySet().forEach(directedAcyclicGraph::addNode);

        paths.keySet().forEach(table -> table.filterColumns(Table.WITH_REF)
                .stream()
                .map(Column::getRef).
                forEach(columnRef -> paths.keySet().forEach(endNode -> {
                    if (endNode.getName().equals(columnRef.getName())) {
                        directedAcyclicGraph.addEdge(table, endNode, columnRef.getName());
                    }
                })));
        paths.keySet().forEach(table -> table.filterColumns(Table.WITH_EACH)
                .stream()
                .map(Column::getEach).
                forEach(each -> paths.keySet().forEach(endNode -> {
                    if (endNode.getName().equals(each.getName())) {
                        directedAcyclicGraph.addEdge(table, endNode, each.getName());
                    }
                })));

        Map<Table, List<Path>> ordered = new LinkedHashMap<>();

        directedAcyclicGraph.topologicalSort(true)
                .forEach(table -> ordered.put(table, paths.get(table)));

        paths.clear();
        paths.putAll(ordered);
    }

    private void createImportIntoFile(Map<Table, List<Path>> paths, ImportSettings importSettings) throws IOException {
        List<String> lines = new ArrayList<>();

        paths.forEach((k, v) -> {
            lines.add(createImportIntoStatement(k, v, importSettings));
            lines.add("");
        });

        Path importFilePath = Paths.get(applicationModel.getOutputPath(), importSettings.getFile());

        Files.write(importFilePath,
                lines,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        ansiConsole.blue("Created import file '%s' in topological order [%s]"
                .formatted(importFilePath,
                        StringUtils.collectionToCommaDelimitedString(
                                paths.keySet().stream().map(Table::getName)
                                        .collect(Collectors.toList())))
        ).nl();
    }

    private String createImportIntoStatement(Table table, List<Path> paths, ImportSettings importSettings) {
        StringBuilder sb = new StringBuilder();
        sb.append("IMPORT INTO ")
                .append(table.getName())
                .append("(")
                .append(StringUtils.collectionToCommaDelimitedString(table.columnNames()))
                .append(")\nCSV DATA (\n");

        AtomicBoolean first = new AtomicBoolean(true);

        PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}");

        paths.stream()
                .sorted()
                .forEach(path -> {
                    if (!first.get()) {
                        sb.append(",\n");
                    }
                    first.set(false);

                    String prefix = helper.replacePlaceholders(importSettings.getPrefix(),
                            placeholderName -> switch (placeholderName) {
                                case "local-ip" -> Networking.getLocalIP();
                                case "external-ip" -> Networking.getPublicIP();
                                default -> placeholderName;
                            });

                    sb.append(" '")
                            .append(prefix)
                            .append(path.getFileName())
                            .append("'");
                });

        sb.append("\n)");

        if (!importSettings.getOptions().isEmpty()) {
            sb.append(" WITH ");

            AtomicBoolean f = new AtomicBoolean(true);

            importSettings.getOptions().forEach((k, v) -> {
                if (!f.get()) {
                    sb.append(", ");
                }
                sb.append(k);
                if (StringUtils.hasLength(v)) {
                    if (!"null".equalsIgnoreCase(v)) {
                        sb.append(" = '").append(v).append("'");
                    }
                    f.set(false);
                }
            });
        }

        sb.append(";");

        return sb.toString();
    }
}