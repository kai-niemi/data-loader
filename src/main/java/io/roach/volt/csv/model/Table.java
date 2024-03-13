package io.roach.volt.csv.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Table {
    public static final Predicate<Column> WITH_REF = column -> column.getRef() != null;

    public static final Predicate<Column> WITH_EACH = column -> column.getEach() != null;

    @NotNull
    private String name;

    private String count;

    private int files = 1;

    @JsonIgnore
    private long finalCount;

    @NotEmpty
    private List<Column> columns = new ArrayList<>();

    public List<Column> filterColumns(Predicate<Column> filter) {
        return columns.stream().filter(filter).toList();
    }

    public List<String> columnNames() {
        return columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    @JsonIgnore
    public long getFinalCount() {
        return finalCount;
    }

    public void setFinalCount(long finalCount) {
        this.finalCount = finalCount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCount() {
        return count;
    }

    public int getFiles() {
        return files;
    }

    public void setFiles(int files) {
        this.files = files;
    }

    public void setCount(String count) {
        this.count = count;
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
