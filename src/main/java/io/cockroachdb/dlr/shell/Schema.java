package io.cockroachdb.dlr.shell;

import io.cockroachdb.dlr.schema.ForeignKeyModel;
import io.cockroachdb.dlr.schema.MetaDataUtils;
import io.cockroachdb.dlr.schema.TableModel;
import io.cockroachdb.dlr.shell.support.AnotherFileValueProvider;
import io.cockroachdb.dlr.shell.support.AnsiConsole;
import io.cockroachdb.dlr.shell.support.TableNameProvider;
import io.cockroachdb.dlr.shell.support.TableUtils;
import io.cockroachdb.dlr.util.graph.DirectedAcyclicGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Set;

@ShellComponent
@ShellCommandGroup(CommandGroups.SCHEMA)
public class Schema {
    @Autowired
    private DataSource dataSource;

    @Autowired
    private AnsiConsole ansiConsole;

    @ShellMethod(value = "Execute SQL file", key = {"db-exec", "dbe"})
    public void createSchema(@ShellOption(help = "path to DDL/DML file",
            valueProvider = AnotherFileValueProvider.class) String sql) {

        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new FileSystemResource(sql));
        populator.setCommentPrefixes("--", "#");
        populator.setIgnoreFailedDrops(false);
        populator.setContinueOnError(false);

        ansiConsole.magenta("%s", sql).nl();

        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @ShellMethod(value = "List tables", key = {"db-tables", "dbt"})
    public void listTables(@ShellOption(
            help = "table name(s)",
            defaultValue = "*",
            valueProvider = TableNameProvider.class) String tableNames) {
        Set<String> names = StringUtils.commaDelimitedListToSet(tableNames.toLowerCase());

        MetaDataUtils.listTables(dataSource, resultSet -> {
            try {
                ansiConsole.cyan(TableUtils.prettyPrint(resultSet, rs -> {
                    try {
                        return names.contains("*")
                                || names.contains(rs.getString("TABLE_NAME").toLowerCase());
                    } catch (SQLException e) {
                        return false;
                    }
                }));
            } catch (SQLException e) {
                throw new CommandException(e);
            }
        });
    }

    @ShellMethod(value = "List columns", key = {"db-columns", "dbc"})
    public void listColumns(@ShellOption(
            help = "table name",
            defaultValue = "*",
            valueProvider = TableNameProvider.class) String tableName) {
        MetaDataUtils.tableNames(dataSource, tableName)
                .forEach(name -> {
            ansiConsole.magenta("%s:", name).nl();
            MetaDataUtils.listColumns(dataSource, name, resultSet -> {
                try {
                    ansiConsole.cyan(TableUtils.prettyPrint(resultSet)).nl();
                } catch (SQLException e) {
                    throw new CommandException(e);
                }
            });
        });
    }

    @ShellMethod(value = "Show create table", key = {"db-show-table", "dbs"})
    public void showCreateTable(@ShellOption(
            help = "table name",
            defaultValue = "*",
            valueProvider = TableNameProvider.class) String tableName) {
        MetaDataUtils.tableNames(dataSource, tableName)
                .forEach(name -> {
            ansiConsole.cyan(MetaDataUtils.showCreateTable(dataSource, name)).nl().nl();
        });
    }

    @ShellMethod(value = "List foreign keys", key = {"db-foreign-keys", "dbfk"})
    public void listForeignKeys(@ShellOption(
            help = "table name",
            defaultValue = "*",
            valueProvider = TableNameProvider.class) String tableName) {
        MetaDataUtils.tableNames(dataSource, tableName)
                .forEach(name -> {
            MetaDataUtils.listForeignKeys(dataSource, name, resultSet -> {
                try {
                    ansiConsole.cyan(TableUtils.prettyPrint(resultSet)).nl();
                } catch (SQLException e) {
                    throw new CommandException(e);
                }
            });
        });
    }

    @ShellMethod(value = "List primary keys", key = {"db-primary-keys", "dbpk"})
    public void listPrimaryKeys(@ShellOption(
            help = "table name",
            defaultValue = "*",
            valueProvider = TableNameProvider.class) String tableName) {
        MetaDataUtils.tableNames(dataSource, tableName)
                .forEach(name -> {
            MetaDataUtils.listPrimaryKeys(dataSource, name, resultSet -> {
                try {
                    ansiConsole.cyan(TableUtils.prettyPrint(resultSet));
                } catch (SQLException e) {
                    throw new CommandException(e);
                }
            });
        });
    }

    @ShellMethod(value = "List table topology", key = {"db-topology", "dby"})
    public void listTopology(@ShellOption(
            help = "table name(s)",
            defaultValue = "*",
            valueProvider = TableNameProvider.class) String tableNames) {

        final Set<String> names = StringUtils.commaDelimitedListToSet(tableNames.toLowerCase());

        final DirectedAcyclicGraph<String, ForeignKeyModel> directedAcyclicGraph = new DirectedAcyclicGraph<>();

        MetaDataUtils.listTables(dataSource, resultSet -> {
            while (resultSet.next()) {
                TableModel table = new TableModel(resultSet);

                if (names.contains("*") || names.contains(table.getName())) {
                    directedAcyclicGraph.addNode(table.getName());
                }

                MetaDataUtils.listForeignKeys(dataSource, table.getName(), rs -> {
                    while (rs.next()) {
                        ForeignKeyModel fk = new ForeignKeyModel(rs);
                        directedAcyclicGraph.addNode(fk.getPkTableName());
                        directedAcyclicGraph.addEdge(fk.getFkTableName(), fk.getPkTableName(), fk);
                    }
                });
            }
        });

        ansiConsole.cyan(directedAcyclicGraph.toString())
                .nl()
                .green("Topological order (inverse):")
                .nl()
                .cyan(directedAcyclicGraph.topologicalSort(true).toString())
                .nl();
    }
}
