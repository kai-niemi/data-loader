package io.cockroachdb.volt.schema;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface ResultSetHandler {
    void process(ResultSet rs) throws SQLException;
}
