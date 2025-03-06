package io.cockroachdb.volt.schema;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cockroachdb.volt.csv.model.Column;
import io.cockroachdb.volt.csv.model.Each;
import io.cockroachdb.volt.csv.model.Gen;
import io.cockroachdb.volt.csv.model.Ref;
import io.cockroachdb.volt.csv.model.Table;
import io.cockroachdb.volt.util.graph.DirectedAcyclicGraph;

import static io.cockroachdb.volt.csv.model.IdentityType.sequence;
import static io.cockroachdb.volt.csv.model.IdentityType.unordered;
import static io.cockroachdb.volt.csv.model.IdentityType.uuid;

public abstract class ModelExporter {
    private static final Logger logger = LoggerFactory.getLogger(ModelExporter.class);

    private ModelExporter() {
    }

    public static DirectedAcyclicGraph<Table, ForeignKeyModel> exportModel(DataSource dataSource,
                                                                           String filter) {
        return exportModel(dataSource, MetaDataUtils.tableNames(dataSource, filter));
    }

    public static DirectedAcyclicGraph<Table, ForeignKeyModel> exportModel(DataSource dataSource,
                                                                           List<String> tableNames) {
        final Map<String, Table> tables = new HashMap<>();
        final Map<String, List<ColumnModel>> columnModels = new HashMap<>();

        // First pass, introspect tables and columns
        tableNames.forEach(name -> {
            List<Column> columns = new ArrayList<>();

            columnModels.put(name, MetaDataUtils.listColumns(dataSource, name));

            columnModels.get(name).forEach(columnModel -> {
                if (!columnModel.isGenerated()) {
                    Column column = new Column();
                    column.setName(columnModel.getName());
                    column.setExpression(ModelExporter.getTypeSpecificExpression(dataSource, columnModel));

                    columns.add(column);
                }
            });

            Table table = new Table();
            table.setName(name);
            table.setCount("100");
            table.setColumns(columns);

            tables.put(name, table);

            logger.info("Found table '%s' with %d columns".formatted(name, columns.size()));
        });


        // Second pass, resolve foreign key refs and halt if not a DAG
        final DirectedAcyclicGraph<Table, ForeignKeyModel> dag = new DirectedAcyclicGraph<>();

        // Build DAG from fk relations
        tableNames.forEach(tName -> MetaDataUtils.listForeignKeys(dataSource, tName, rs -> {
            logger.info("Resolving foreign keys for table '%s'".formatted(tName));

            final Table from = tables.get(tName);
            dag.addNode(from);

            int keys = 0;
            while (rs.next()) {
                ForeignKeyModel fk = new ForeignKeyModel(rs);

                keys++;

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

                Table to = tables.get(fk.getPkTableName());
                if (to == null) {
                    logger.warn("Foreign key '%s' table '%s' not found"
                            .formatted(fk.getName(), fk.getPkTableName()));
                } else {
                    logger.debug("Adding edge for foreign key: %s".formatted(fk));
                    dag.addEdge(from, to, fk);
                }
            }

            if (keys == 0) {
                logger.debug("No foreign keys found for '%s'".formatted(tName));
            }
        }));

        // Third pass, adjust primary key gens

        tableNames.forEach(tName -> MetaDataUtils.listPrimaryKeys(dataSource, tName, rs -> {
            logger.debug("Resolving primary keys for table '%s'".formatted(tName));

            while (rs.next()) {
                PrimaryKeyModel pk = new PrimaryKeyModel(rs);

                logger.debug("Found primary key: %s".formatted(pk));

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
            }
        });

        return dag;
    }

    private static void configureRowId(Column column, ColumnModel model) {
        int dataType = model.getAttribute("DATA_TYPE", Integer.class);

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

        logger.debug("Configured row id gen '%s' for column '%s'"
                .formatted(column.getGen(), column.getName()));
    }

    public static String getTypeSpecificExpression(DataSource dataSource, ColumnModel model) {
        int dataType = model.getAttribute("DATA_TYPE", Integer.class);
        String typeName = model.getAttribute("TYPE_NAME", String.class);
        int columnSize = model.getAttribute("COLUMN_SIZE", Integer.class);
        String columnName = model.getAttribute("COLUMN_NAME", String.class);

        JDBCType type = JDBCType.valueOf(dataType);

        Set<String> enumValues = MetaDataUtils.selectEnumValues(dataSource, typeName);
        if (!enumValues.isEmpty()) {
            return "selectRandom(" + String.join(",", enumValues) + ")";
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
