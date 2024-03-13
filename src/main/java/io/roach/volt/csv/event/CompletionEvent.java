package io.roach.volt.csv.event;

import io.roach.volt.csv.model.Table;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CompletionEvent {
    private final Map<Table, List<Path>> paths;

    public CompletionEvent(Map<Table, List<Path>> paths) {
        this.paths = new LinkedHashMap<>(paths);
    }

    public Map<Table, List<Path>> getPaths() {
        return paths;
    }
}
