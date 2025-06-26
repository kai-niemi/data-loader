package io.cockroachdb.dl.web.model;

import java.util.ArrayList;
import java.util.List;

import org.springframework.hateoas.RepresentationModel;
import org.springframework.hateoas.server.core.Relation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import io.cockroachdb.dl.core.model.Column;
import io.cockroachdb.dl.web.LinkRelations;

import jakarta.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Relation(value = LinkRelations.TABLE_FORM_REL)
@JsonPropertyOrder({"links", "embedded"})
public class TableModel extends RepresentationModel<TableModel> {
    @NotNull
    private String table;

    @NotNull
    private String rows;

    private String note;

    private String importInto;

    private List<Column> columns = new ArrayList<>();

    private boolean includeHeader;

    private boolean ignoreForeignKeys = true;

    private boolean gzip;

    private String delimiter = ",";

    private String quoteCharacter = "";

    public String getImportInto() {
        return importInto;
    }

    public void setImportInto(String importInto) {
        this.importInto = importInto;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public boolean isGzip() {
        return gzip;
    }

    public void setGzip(boolean gzip) {
        this.gzip = gzip;
    }

    public boolean isIgnoreForeignKeys() {
        return ignoreForeignKeys;
    }

    public void setIgnoreForeignKeys(boolean ignoreForeignKeys) {
        this.ignoreForeignKeys = ignoreForeignKeys;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getQuoteCharacter() {
        return quoteCharacter;
    }

    public void setQuoteCharacter(String quoteCharacter) {
        this.quoteCharacter = quoteCharacter;
    }

    public boolean isIncludeHeader() {
        return includeHeader;
    }

    public void setIncludeHeader(boolean includeHeader) {
        this.includeHeader = includeHeader;
    }

    public String getRows() {
        return rows;
    }

    public void setRows(String rows) {
        this.rows = rows;
    }

    public @NotNull String getTable() {
        return table;
    }

    public void setTable(@NotNull String table) {
        this.table = table;
    }
}
