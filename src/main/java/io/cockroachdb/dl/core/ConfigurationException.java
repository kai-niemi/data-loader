package io.cockroachdb.dl.core;

import io.cockroachdb.dl.core.model.Column;
import io.cockroachdb.dl.core.model.Table;

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
