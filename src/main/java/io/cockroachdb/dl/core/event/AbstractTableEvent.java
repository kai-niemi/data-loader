package io.cockroachdb.dl.core.event;

import io.cockroachdb.dl.core.model.Table;

import java.nio.file.Path;

public abstract class AbstractTableEvent {
    private final Table table;

    private final Path path;

    public AbstractTableEvent(Table table, Path path) {
        this.table = table;
        this.path = path;
    }

    public Table getTable() {
        return table;
    }

    public Path getPath() {
        return path;
    }

    public boolean isBoundedCount() {
        return getTable().getFinalCount() > 0;
    }
}
