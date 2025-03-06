package io.cockroachdb.volt.web;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.mediatype.Affordances;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import io.cockroachdb.volt.config.ProfileNames;
import io.cockroachdb.volt.csv.model.Column;
import io.cockroachdb.volt.csv.model.Gen;
import io.cockroachdb.volt.csv.model.Table;
import io.cockroachdb.volt.schema.MetaDataUtils;
import io.cockroachdb.volt.schema.ModelExporter;
import io.cockroachdb.volt.web.csv.CsvStreamUtil;
import io.cockroachdb.volt.web.csv.ImportScriptUtil;
import jakarta.validation.Valid;

import static io.cockroachdb.volt.csv.model.IdentityType.uuid;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.afford;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
@Profile(ProfileNames.HTTP)
public class TableController {
    @Autowired
    private DataSource dataSource;

    @GetMapping(value = "/table")
    public ResponseEntity<MessageModel> index() {
        MessageModel index = MessageModel.from("Table index");

        index.add(linkTo(methodOn(getClass())
                .index())
                .withSelfRel());

        MetaDataUtils.tableNames(dataSource, "*").forEach(name -> {
            index.add(linkTo(methodOn(getClass())
                    .exportTable(name, null, ""))
                    .withRel(LinkRelations.TABLE_SCHEMA_REL)
                    .withType(MediaType.TEXT_PLAIN_VALUE)
                    .withTitle("Generate CSV stream from table schema"));
            index.add(linkTo(methodOn(getClass())
                    .getForm(name))
                    .withRel(LinkRelations.TABLE_FORM_REL)
                    .withType(MediaTypes.HAL_FORMS_JSON_VALUE)
                    .withTitle("Table form for generating CSV stream"));
        });

        return ResponseEntity.ok(index);
    }

    @GetMapping(value = "/table/form/{table}")
    public ResponseEntity<TableModel> getForm(@PathVariable(name = "table") String table) {
        TableModel tableModel = new TableModel();
        tableModel.setTable(table);
        tableModel.setRows("100");
        tableModel.setIncludeHeader(true);

        Set<Table> tables = ModelExporter.exportModel(dataSource, table).nodes();

        if (!tables.isEmpty()) {
            Table t = tables.stream().findFirst().orElseThrow();

            tableModel.getColumns().addAll(t.getColumns());
            tableModel.setRows(t.getCount());
            tableModel.setImportInto(ImportScriptUtil.generateImportInto(tableModel));
        } else {
            tableModel.setNote("Table not found: " + table);

            Column c1 = Column.of("id");
            c1.setGen(Gen.of(uuid));

            Column c2 = Column.of("name");
            c2.setExpression("randomString(32)");

            tableModel.getColumns().add(c1);
            tableModel.getColumns().add(c2);
        }

        tableModel.add(Affordances.of(linkTo(methodOn(getClass())
                        .getForm(table))
                        .withSelfRel()
                        .andAffordance(afford(methodOn(getClass())
                                .submitForm(null)))
                        .withType(MediaType.APPLICATION_JSON_VALUE)
                        .withTitle("Generate CSV stream from table form"))
                .toLink());

        return ResponseEntity.ok(tableModel);
    }

    @PostMapping(value = "/table/form",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<StreamingResponseBody> submitForm(@Valid @RequestBody TableModel tableModel) {
        ResponseEntity.BodyBuilder bb = ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN_VALUE)
                .header(HttpHeaders.CONTENT_DISPOSITION, "inline")
                .header("Cache-Control", "no-cache, no-store, must-revalidate")
                .header("Pragma", "no-cache")
                .header("Expires", "0");
        if (tableModel.isGzip()) {
            bb.header(HttpHeaders.CONTENT_ENCODING, "gzip");
        }
        return bb.body(outputStream -> CsvStreamUtil.writeCsvStream(dataSource, tableModel, outputStream));
    }

    @GetMapping(value = "/table/schema/{table}",
            produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<StreamingResponseBody> exportTable(
            @PathVariable(name = "table") String table,
            @RequestParam(required = false) MultiValueMap<String, String> valueMap,
            @RequestHeader(value = HttpHeaders.ACCEPT_ENCODING,
                    required = false, defaultValue = "") String acceptEncoding) {
        Map<String, String> allParams = Objects.requireNonNull(valueMap, "params required").toSingleValueMap();

        TableModel tableModel = new TableModel();
        tableModel.setTable(table);
        tableModel.setRows(allParams.getOrDefault("rows", "100"));
        tableModel.setDelimiter(allParams.getOrDefault("delimiter", ","));
        tableModel.setQuoteCharacter(allParams.getOrDefault("quoteCharacter", ""));
        tableModel.setIncludeHeader(Boolean.parseBoolean(allParams.getOrDefault("header", "false")));
        tableModel.setGzip(acceptEncoding.contains("gzip"));

        Set<Table> tables = ModelExporter.exportModel(dataSource, table).nodes();
        Table t = tables.stream().findFirst()
                .orElseThrow(() -> new NotFoundException("No such table: " + table));

        tableModel.getColumns().addAll(t.getColumns());

        ResponseEntity.BodyBuilder bb = ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN_VALUE)
                .header(HttpHeaders.CONTENT_DISPOSITION, "inline")
                .header("Cache-Control", "no-cache, no-store, must-revalidate")
                .header("Pragma", "no-cache")
                .header("Expires", "0");
        if (tableModel.isGzip()) {
            bb.header(HttpHeaders.CONTENT_ENCODING, "gzip");
        }

        return bb.body(outputStream -> CsvStreamUtil.writeCsvStream(dataSource, tableModel, outputStream));
    }
}
