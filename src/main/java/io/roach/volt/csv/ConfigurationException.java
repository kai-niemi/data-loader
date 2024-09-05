package io.roach.volt.csv;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Table;

public class ConfigurationException extends RuntimeException {
    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Table table) {
        super("Configuration error for table '" + table.getName() + "': " + message);
    }

    public ConfigurationException(String message, Column column) {
        super("Configuration error for column '" + column.getName() + "': " + message);
    }
}
