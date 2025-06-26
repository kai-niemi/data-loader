package io.cockroachdb.dl.core.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.cockroachdb.dl.util.Multiplier;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public class Table {
    public static final Predicate<Column> WITH_REF = column -> column.getRef() != null;

    public static final Predicate<Column> WITH_EACH = column -> column.getEach() != null;

    @NotNull
    private String name;

    @Pattern(regexp = "^[+-]?([0-9]+\\.?[0-9]*|\\.[0-9]+)\\s?([kKmMgG]+)?")
    private String count;

    @NotEmpty
    private List<Column> columns = new ArrayList<>();

    @JsonProperty("options")
    private Map<ImportOption, String> options = new LinkedHashMap<>();

    public Map<ImportOption, String> getOptions() {
        return options;
    }

    public void setOptions(Map<ImportOption, String> options) {
        this.options = options;
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

    @JsonIgnore
    public int getFinalCount() {
        return count != null ? Multiplier.parseInt(count) : 0;
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

    public List<Column> filterColumns(Predicate<Column> filter) {
        return columns.stream().filter(filter).toList();
    }

    public List<String> columnNames() {
        return columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table table = (Table) o;
        return Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", count='" + count + '\'' +
                '}';
    }
}
