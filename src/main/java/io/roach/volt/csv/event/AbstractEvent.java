package io.roach.volt.csv.event;

import io.roach.volt.csv.model.Table;

import java.nio.file.Path;

public abstract class AbstractEvent {
    private final Table table;

    private final Path path;

    public AbstractEvent(Table table, Path path) {
        this.table = table;
        this.path = path;
    }

    public Table getTable() {
        return table;
    }

    public Path getPath() {
        return path;
    }
}
