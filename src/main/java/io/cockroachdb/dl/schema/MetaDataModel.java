package io.cockroachdb.dl.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public abstract class MetaDataModel {
    private final Map<String, Object> attributes = new TreeMap<>();

    public MetaDataModel(ResultSet resultSet) throws SQLException {
        java.sql.ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            attributes.put(resultSetMetaData.getColumnName(i).toLowerCase(), resultSet.getObject(i));
        }
    }

    public abstract String getName();

    public <T> T getAttribute(String name, Class<T> type) {
        return type.cast(attributes.get(name.toLowerCase()));
    }

    public <T> T getAttribute(String name, Class<T> type, T defaultValue) {
        return type.cast(attributes.getOrDefault(name.toLowerCase(), defaultValue));
    }

    public Map<String, Object> getNotNullAttributes() {
        return attributes.entrySet()
                .stream().filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> b, LinkedHashMap::new));
    }

    @Override
    public String toString() {
        return "ResultSetMetaData{" +
                "attributes=" + getNotNullAttributes() +
                '}';
    }
}
