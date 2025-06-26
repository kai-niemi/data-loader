package io.cockroachdb.dl.core.generator;

import io.cockroachdb.dl.core.model.Gen;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;
import java.util.LinkedList;

public class DatabaseSequenceGenerator implements ValueGenerator<Long> {
    private final DataSource dataSource;

    private final String sequence;

    private final int batchSize;

    private final Deque<Long> uniqueRowIdBuffer = new LinkedList<>();

    public DatabaseSequenceGenerator(DataSource dataSource, Gen gen) {
        this.dataSource = dataSource;
        this.sequence = gen.getSequence();
        this.batchSize = gen.getBatchSize();
    }

    @Override
    public Long nextValue() {
        if (uniqueRowIdBuffer.isEmpty()) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "select nextval('" + sequence + "') from generate_series(1, " + batchSize + ")")) {
                ps.setFetchSize(batchSize);
                try (ResultSet res = ps.executeQuery()) {
                    while (res.next()) {
                        uniqueRowIdBuffer.add(res.getLong(1));
                    }
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return uniqueRowIdBuffer.pop();
    }
}
