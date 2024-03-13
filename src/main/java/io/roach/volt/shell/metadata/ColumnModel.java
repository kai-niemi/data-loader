package io.roach.volt.shell.metadata;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnModel extends MetaDataModel {
    private String comment;

    private String expression;

    public ColumnModel(ResultSet resultSet) throws SQLException {
        super(resultSet);
    }

    public boolean isGenerated() {
        return getAttribute("is_generatedcolumn", String.class, "NO")
                .equalsIgnoreCase("YES");
    }

    public String getExpression() {
        return expression;
    }

    public ColumnModel setExpression(String expression) {
        this.expression = expression;
        return this;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String getName() {
        return getAttribute("COLUMN_NAME", String.class);
    }
}
