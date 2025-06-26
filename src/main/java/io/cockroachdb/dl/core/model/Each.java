package io.cockroachdb.dl.core.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Each {
    private String name;

    private String column;

    private Integer multiplier = 1;

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

    public Integer getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(Integer multiplier) {
        this.multiplier = multiplier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Each each = (Each) o;
        return Objects.equals(name, each.name) && Objects.equals(column, each.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, column);
    }

    @Override
    public String toString() {
        return "Each{" +
                "column='" + column + '\'' +
                ", name='" + name + '\'' +
                ", multiplier=" + multiplier +
                '}';
    }
}
