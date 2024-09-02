package io.roach.volt.csv;

import io.roach.volt.csv.model.Table;

public class TableConfigException extends ModelConfigException {
    private final Table table;

    public TableConfigException(String message, Table table) {
        super("Configuration error for table '" + table.getName() + "': " + message);
        this.table = table;
    }

    public Table getTable() {
        return table;
    }
}
