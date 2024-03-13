package io.roach.volt.csv.model;

import jakarta.validation.constraints.NotNull;

import java.util.Objects;

public class Ref {
    @NotNull
    private String name;

    @NotNull
    private String column;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Ref ref = (Ref) o;
        return Objects.equals(name, ref.name) && Objects.equals(column, ref.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, column);
    }
}
