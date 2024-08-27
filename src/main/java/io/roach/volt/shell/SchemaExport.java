package io.roach.volt.shell;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.shell.standard.ShellCommandGroup;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.roach.volt.csv.model.ApplicationModel;
import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Each;
import io.roach.volt.csv.model.Gen;
import io.roach.volt.csv.model.Ref;
import io.roach.volt.csv.model.Root;
import io.roach.volt.csv.model.SpringModel;
import io.roach.volt.csv.model.Table;
import io.roach.volt.shell.metadata.ColumnModel;
import io.roach.volt.shell.metadata.ForeignKeyModel;
import io.roach.volt.shell.metadata.MetaDataUtils;
import io.roach.volt.shell.metadata.PrimaryKeyModel;
import io.roach.volt.shell.support.TableNameProvider;
import io.roach.volt.util.graph.DirectedAcyclicGraph;

import static io.roach.volt.csv.model.IdentityType.sequence;
import static io.roach.volt.csv.model.IdentityType.unordered;
import static io.roach.volt.csv.model.IdentityType.uuid;

@ShellComponent
@ShellCommandGroup(CommandGroups.SCHEMA)
public class SchemaExport {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DataSource dataSource;

    @Autowired
    @Qualifier("yamlObjectMapper")
    private ObjectMapper yamlObjectMapper;

    @ShellMethod(value = "Export table schema to YAML model", key = {"db-export", "dbx"})
    public void exportModel(
            @ShellOption(help = "table name", defaultValue = "*", valueProvider = TableNameProvider.class)
            String tableName,
            @ShellOption(help = "output path relative to base dir (created on demand)", defaultValue = ".output")
            String outputPath,
            @ShellOption(help = "target application YAML file name",
                    defaultValue = "application-default.yml") String outputFile

    ) throws IOException {
        final Map<String, Table> tables = new HashMap<>();
        final Map<String, List<ColumnModel>> columnModels = new HashMap<>();

        final List<String> tableNames = MetaDataUtils.tableNames(dataSource, tableName);

        tableNames.forEach(name -> {
            List<Column> columns = new ArrayList<>();

            columnModels.put(name, MetaDataUtils.listColumns(dataSource, name));

            columnModels.get(name).forEach(columnModel -> {
                if (!columnModel.isGenerated()) {
                    Column column = new Column();
                    column.setName(columnModel.getName());
                    column.setExpression(getTypeSpecificExpression(dataSource, columnModel));

                    columns.add(column);
                }
            });

            Table table = new Table();
            table.setName(name);
            table.setCount("100");
            table.setColumns(columns);

            tables.put(name, table);

            logger.info("Found table '%s'".formatted(name));
        });

        // Build DAG from fk relations
        final DirectedAcyclicGraph<Table, ForeignKeyModel> dag = new DirectedAcyclicGraph<>();

        // Second pass, resolve foreign key refs and halt if not a DAG
        tableNames.forEach(name -> MetaDataUtils.listForeignKeys(dataSource, name, rs -> {
            logger.info("Resolving foreign keys for '%s'".formatted(name));

            final Table from = tables.get(name);
            dag.addNode(from);

            int keys = 0;
            while (rs.next()) {
                keys++;

                ForeignKeyModel fk = new ForeignKeyModel(rs);

                Table to = tables.get(fk.getPkTableName());
                if (to == null) {
                    throw new IllegalStateException("Foreign key '%s' table '%s' not found"
                            .formatted(fk.getName(), fk.getPkTableName()));
                }

                for (Column column : from.getColumns()) {
                    if (column.getName().equals(fk.getFkColumnName())) {
                        long seq = fk.getAttribute("key_seq", Long.class);
                        if (seq <= 1) {
                            Each each = new Each();
                            each.setName(fk.getPkTableName());
                            each.setColumn(fk.getPkColumnName());
                            each.setMultiplier(1);

                            column.setEach(each);
                        } else {
                            Ref ref = new Ref();
                            ref.setName(fk.getPkTableName());
                            ref.setColumn(fk.getPkColumnName());

                            column.setRef(ref);
                        }
                        column.setExpression(null);
                        break;
                    }
                }

                logger.debug("Foreign key found: %s".formatted(fk));

                dag.addEdge(from, to, fk);
            }

            if (keys == 0) {
                logger.debug("No foreign keys found");
            }
        }));

        // Third pass, adjust primary key gens
        tableNames.forEach(name -> MetaDataUtils.listPrimaryKeys(dataSource, name, rs -> {
            logger.debug("Resolving primary keys for '%s'".formatted(name));

            while (rs.next()) {
                PrimaryKeyModel pk = new PrimaryKeyModel(rs);

                logger.debug("Primary key: %s".formatted(pk));

                Table table = tables.get(pk.getTableName());
                table.getColumns()
                        .stream()
                        .filter(column -> column.getName().equals(pk.getColumnName()))
                        .findFirst().ifPresent(column -> {
                            columnModels.get(pk.getTableName())
                                    .stream()
                                    .filter(columnModel -> columnModel.getName().equals(pk.getColumnName()))
                                    .findFirst().ifPresent(columnModel -> {
                                        if (column.getRef() == null && column.getEach() == null) {
                                            configureRowId(column, columnModel);
                                        }
                                    });
                        });
            }
        }));

        tables.values().forEach(table -> {
            if ((long) table.filterColumns(Table.WITH_EACH).size() > 0) {
                table.setCount(null);
                table.setFinalCount(0);
            }
        });

        exportModel(dag.topologicalSort(true), outputPath, outputFile);
    }

    private void configureRowId(Column column, ColumnModel model) {
        int dataType = model.getAttribute("DATA_TYPE", Integer.class);

        logger.debug("Configure row id for column '%s'".formatted(column.getName()));

        switch (JDBCType.valueOf(dataType)) {
            case TINYINT, SMALLINT, INTEGER, BIGINT -> {
                String autoincrement = model.getAttribute("IS_AUTOINCREMENT", String.class);
                String generatedColumn = model.getAttribute("IS_GENERATEDCOLUMN", String.class);
//                https://www.cockroachlabs.com/docs/v23.2/functions-and-operators#id-generation-functions
                String columnDef = model.getAttribute("COLUMN_DEF", String.class);

                Gen gen = new Gen();

                if ("unique_rowid()".equalsIgnoreCase(columnDef)
                        || "unordered_unique_rowid()".equalsIgnoreCase(columnDef)) {
                    gen.setType(unordered);
                    gen.setBatchSize(512);
                } else if ("gen_random_uuid()".equalsIgnoreCase(columnDef)
                        || "uuid_generate_v4()".equalsIgnoreCase(columnDef)) {
                    gen.setType(uuid);
                } else {
                    if ("YES".equalsIgnoreCase(generatedColumn)) {
                        gen.setType(unordered);
                        gen.setBatchSize(512);
                    } else {
                        gen.setType(sequence);
                        gen.setFrom("1");
                        gen.setStep(1);
                    }
                }

                column.setGen(gen);
                column.setExpression(null);
            }
            case OTHER -> {
                Gen id = new Gen();
                id.setType(uuid);

                column.setGen(id);
                column.setExpression(null);
            }
        }
    }

    @Value("${spring.datasource.url}")
    private String url;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    private void exportModel(List<Table> tables, String outputDir, String outputFile) throws IOException {
        final ApplicationModel model = new ApplicationModel();
        model.setOutputPath(outputDir);
        model.setTables(tables);

        logger.info("Topological order (inverse): %s".formatted(
                String.join(",", tables.stream().map(Table::getName).toList())));

        if (!"".equalsIgnoreCase(outputFile)) {
            Path outputPath = Paths.get(outputDir);
            if (!Files.isDirectory(outputPath)) {
                logger.debug("Creating directory: %s".formatted(outputPath));
                Files.createDirectories(outputPath);
            }

            final Path modelFile = outputPath.resolve(outputFile);
            if (Files.isRegularFile(modelFile)) {
                Path backup = outputPath.resolve(outputFile + ".backup");
                if (!Files.isRegularFile(backup)) {
                    Files.move(modelFile, backup);
                    logger.info("Created backup file '%s'".formatted(backup));
                } else {
                    logger.warn("Backup file already exist: '%s'".formatted(backup));
                }
            }

            StringWriter sw = new StringWriter();

            yamlObjectMapper.writerFor(Root.class).writeValue(sw,
                    new Root(model).setSpringModel(
                            new SpringModel().setDataSource(
                                    new SpringModel.DataSource()
                                            .setUrl(url)
                                            .setUsername(username)
                                            .setPassword(password)))
            );

            logger.info(sw.toString());

            logger.info("Writing model file '%s'".formatted(modelFile));

            Files.writeString(modelFile, sw.toString(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            logger.info("NOTE: Restart to apply exported model");
        }
    }

    public static String getTypeSpecificExpression(DataSource dataSource, ColumnModel model) {
        int dataType = model.getAttribute("DATA_TYPE", Integer.class);
        String typeName = model.getAttribute("TYPE_NAME", String.class);
        int columnSize = model.getAttribute("COLUMN_SIZE", Integer.class);
        String columnName = model.getAttribute("COLUMN_NAME", String.class);

        JDBCType type = JDBCType.valueOf(dataType);

        Set<String> enumValues = MetaDataUtils.selectEnumValues(dataSource, typeName);
        if (!enumValues.isEmpty()) {
            return "selectRandom(" + StringUtils.collectionToCommaDelimitedString(enumValues) + ")";
        }

        switch (type) {
            case BIT, BOOLEAN -> {
                return "randomBoolean()";
            }
            case TINYINT, SMALLINT, INTEGER -> {
                return "randomInt()";
            }
            case BIGINT -> {
                return "randomLong()";
            }
            case FLOAT, REAL, DOUBLE -> {
                return "randomDouble()";
            }
            case NUMERIC, DECIMAL -> {
                int digits = model.getAttribute("DECIMAL_DIGITS", Integer.class);
                double bound = Math.pow(10, columnSize - digits);
                return "randomBigDecimal(0,%d,%d)".formatted((long) bound, digits);
            }
            case CHAR, VARCHAR, LONGVARCHAR -> {
                if (columnName.matches("email")) {
                    return "randomEmail()";
                } else if (columnName.matches("city")) {
                    return "randomCity()";
                } else if (columnName.matches("name")) {
                    return "randomFullName()";
                } else if (columnName.matches("country")) {
                    return "randomCountry()";
                } else if (columnName.matches("phone")) {
                    return "randomPhoneNumber()";
                } else if (columnName.matches("state")) {
                    return "randomState()";
                } else if (columnName.matches("zip")) {
                    return "randomZipCode()";
                } else if (columnName.matches("currency")) {
                    return "randomCurrency()";
                }
                return "randomString(%d)".formatted(Math.min(512, columnSize));
            }
            case DATE -> {
                return "randomDate()";
            }
            case TIME -> {
                return "randomTime()";
            }
            case TIMESTAMP -> {
                return "randomDateTime()";
            }
            case BINARY, VARBINARY, LONGVARBINARY -> {
                columnSize = Math.min(512, columnSize);
                return "toBase64(randomBytes(%d))".formatted(columnSize);
            }
            case OTHER -> {
                return "randomJson(1,1)";
            }
        }
        return "unsupported('%s')".formatted(typeName);
    }
}
