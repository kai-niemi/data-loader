package io.cockroachdb.volt.schema;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TableModel extends MetaDataModel {
    private final String name;

    public TableModel(ResultSet resultSet) throws SQLException {
        super(resultSet);
        this.name = resultSet.getString("TABLE_NAME").toLowerCase();
    }

    @Override
    public String getName() {
        return name;
    }
}
