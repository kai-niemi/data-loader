package io.roach.volt.shell.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public abstract class MetaDataUtils {
    protected static final Logger logger = LoggerFactory.getLogger(MetaDataUtils.class);

    private MetaDataUtils() {
    }

    public static List<String> tableNames(DataSource dataSource, String filter) {
        List<String> tables = new ArrayList<>();
        if (filter.equals("*")) {
            listTables(dataSource, rs -> {
                while (rs.next()) {
                    tables.add(new TableModel(rs).getName());
                }
            });
        } else {
            if (!tableExists(dataSource, filter)) {
                throw new IllegalArgumentException("Table not found: " + filter);
            }
            tables.add(filter);
        }
        return tables;
    }

    public static void listTables(DataSource dataSource, ResultSetHandler handler) {
        try (Connection connection = DataSourceUtils.doGetConnection(dataSource);
             ResultSet columns = connection.getMetaData()
                     .getTables(null, "public", null, new String[]{"TABLE"})) {
            handler.process(columns);
        } catch (SQLException ex) {
            throw new DataAccessResourceFailureException("Error reading table metadata", ex);
        }
    }

    public static void listPrimaryKeys(DataSource dataSource, String tableName, ResultSetHandler handler) {
        tableName = stripQuotes(tableName);

        try (Connection connection = DataSourceUtils.doGetConnection(dataSource);
             ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, tableName)) {
            handler.process(resultSet);
        } catch (SQLException ex) {
            throw new DataAccessResourceFailureException("Error reading primary key metadata", ex);
        }
    }

    public static void listForeignKeys(DataSource dataSource, String tableName, ResultSetHandler handler) {
        tableName = stripQuotes(tableName);

        try (Connection connection = DataSourceUtils.doGetConnection(dataSource);
             ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, tableName)) {
            handler.process(resultSet);
        } catch (SQLException ex) {
            throw new DataAccessResourceFailureException("Error reading foreign key metadata", ex);
        }
    }

    public static void listColumns(DataSource dataSource, String tableName, ResultSetHandler handler) {
        tableName = stripQuotes(tableName);

        try (Connection connection = DataSourceUtils.doGetConnection(dataSource);
             ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, null)) {
            handler.process(resultSet);
        } catch (SQLException ex) {
            throw new DataAccessResourceFailureException("Error reading column metadata", ex);
        }
    }

    public static List<ColumnModel> listColumns(DataSource dataSource, String tableName) {
        tableName = stripQuotes(tableName);

        List<ColumnModel> columns = new LinkedList<>();
        listColumns(dataSource, tableName, resultSet -> {
            while (resultSet.next()) {
                columns.add(new ColumnModel(resultSet));
            }
        });

        loadComments(dataSource, tableName, rs -> {
            String columnName = rs.getString("column_name");
            String comment = rs.getString("comment");

            columns.forEach(column -> {
                if (column.getName().equals(columnName)) {
                    column.setComment(comment);
                }
            });
        });

        return columns;
    }

    public static String showCreateTable(DataSource dataSource, String table) {
        JdbcTemplate template = new JdbcTemplate(dataSource);

        String createTable;
        try {
            createTable = template
                    .queryForObject("SELECT create_statement FROM [SHOW CREATE TABLE " + table + "]",
                            String.class).replaceAll("[\r\n\t]+", "");
        } catch (DataAccessException e) {
            // Assuming it's related to escapes since JDBC metadata API removes surrounding quotes
            createTable = template
                    .queryForObject("SELECT create_statement FROM [SHOW CREATE TABLE \"" + table + "\"]",
                            String.class).replaceAll("[\r\n\t]+", "");
        }

        return createTable.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
    }

    public static String databaseVersion(DataSource dataSource) {
        try {
            return new JdbcTemplate(dataSource).queryForObject("select version()", String.class);
        } catch (DataAccessException e) {
            return "unknown";
        }
    }

    public static boolean isCockroachDB(DataSource dataSource) {
        return databaseVersion(dataSource).contains("CockroachDB");
    }

    public static boolean tableExists(DataSource dataSource, String tableName) {
        tableName = stripQuotes(tableName);

        try (Connection connection = DataSourceUtils.doGetConnection(dataSource);
             ResultSet resultSet = connection.getMetaData().getTables(null, null, tableName, new String[]{"TABLE"})) {
            return resultSet.next();
        } catch (SQLException ex) {
            throw new DataAccessResourceFailureException("Error reading table metadata", ex);
        }
    }

    private static String stripQuotes(String tableName) {
        return tableName.replaceAll("\"", "");
    }

    public static void loadComments(DataSource dataSource, String tableName, RowCallbackHandler handler) {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        try {
            template.query("select column_name,comment from [SHOW COLUMNS FROM "
                    + tableName + " WITH COMMENT] where comment is not null", handler);
        } catch (BadSqlGrammarException e) {
            template.query("select column_name,comment from [SHOW COLUMNS FROM \""
                    + tableName + "\" WITH COMMENT] where comment is not null", handler);
        }
    }

    public static Set<String> selectEnumValues(DataSource dataSource, String tableName) {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        try {
            String values = template.queryForObject("select array_to_string(values,',') from [SHOW ENUMS] where name = '"
                    + tableName + "'", String.class);
            Set<String> quotedValues = new HashSet<>();
            StringUtils.commaDelimitedListToSet(values).forEach(s -> quotedValues.add("'" + s + "'"));
            return quotedValues;
        } catch (DataAccessException e) {
            return Set.of();
        }
    }
}
