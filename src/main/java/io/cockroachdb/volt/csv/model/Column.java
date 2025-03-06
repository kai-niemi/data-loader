package io.cockroachdb.volt.csv.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import jakarta.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Column {
    public static Column of(String name) {
        Column column = new Column();
        column.setName(name);
        return column;
    }

    @NotNull
    private String name;

    private String constant;

    private String expression;

    private Each each;

    private Ref ref;

    private Range range;

    private Gen gen;

    private ValueSet<?> set;

    private Boolean hidden;

    public Boolean isHidden() {
        return hidden;
    }

    public void setHidden(Boolean hidden) {
        this.hidden = hidden;
    }

    public ValueSet<?> getSet() {
        return set;
    }

    public void setSet(ValueSet<?> set) {
        this.set = set;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConstant() {
        return constant;
    }

    public void setConstant(String constant) {
        this.constant = constant;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public Each getEach() {
        return each;
    }

    public void setEach(Each each) {
        this.each = each;
    }

    public Ref getRef() {
        return ref;
    }

    public void setRef(Ref ref) {
        this.ref = ref;
    }

    public Range getRange() {
        return range;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    public Gen getGen() {
        return gen;
    }

    public void setGen(Gen gen) {
        this.gen = gen;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Column column = (Column) o;

        return name.equals(column.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "Column{" +
               "constant='" + constant + '\'' +
               ", name='" + name + '\'' +
               ", expression='" + expression + '\'' +
               ", each=" + each +
               ", ref=" + ref +
               ", range=" + range +
               ", gen=" + gen +
               ", set=" + set +
               ", hidden=" + hidden +
               '}';
    }
}
