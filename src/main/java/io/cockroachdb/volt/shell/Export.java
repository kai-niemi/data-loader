package io.cockroachdb.volt.shell;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

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

import com.fasterxml.jackson.databind.ObjectMapper;

import io.cockroachdb.volt.csv.model.ApplicationModel;
import io.cockroachdb.volt.csv.model.ImportInto;
import io.cockroachdb.volt.csv.model.Root;
import io.cockroachdb.volt.csv.model.SpringModel;
import io.cockroachdb.volt.csv.model.Table;
import io.cockroachdb.volt.schema.ForeignKeyModel;
import io.cockroachdb.volt.schema.ModelExporter;
import io.cockroachdb.volt.shell.support.TableNameProvider;
import io.cockroachdb.volt.util.graph.DirectedAcyclicGraph;

@ShellComponent
@ShellCommandGroup(CommandGroups.SCHEMA)
public class Export {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DataSource dataSource;

    @Autowired
    @Qualifier("yamlObjectMapper")
    private ObjectMapper yamlObjectMapper;

    @Value("${spring.datasource.url}")
    private String url;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @ShellMethod(value = "Export table schema to CSV layout / application model", key = {"db-export", "dbx"})
    public void exportModel(
            @ShellOption(help = "table name", defaultValue = "*", valueProvider = TableNameProvider.class)
            String tableName,
            @ShellOption(help = "output path relative to base dir", defaultValue = ".output")
            String outputDir,
            @ShellOption(help = "application YAML file name", defaultValue = "application-default.yml")
            String outputFile

    ) throws IOException {
        final DirectedAcyclicGraph<Table, ForeignKeyModel> dag = ModelExporter.exportModel(dataSource, tableName);

        List<Table> tables = dag.topologicalSort(true);

        final ApplicationModel model = new ApplicationModel();
        model.setOutputPath(outputDir);
        model.setTables(tables);
        model.setImportInto(ImportInto.createDefault());
        model.getOptions().putAll(ImportInto.createDefaultOptions());

        logger.info("Topological order (inverse): %s".formatted(
                String.join(",", tables.stream().map(Table::getName).toList()))
        );

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
            logger.info("Writing model to file '%s'".formatted(modelFile));

            Files.writeString(modelFile, sw.toString(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            logger.info("NOTE: Restart to apply exported model");
        }
    }
}
