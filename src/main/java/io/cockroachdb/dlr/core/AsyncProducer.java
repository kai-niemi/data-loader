package io.cockroachdb.dlr.core;

import javax.sql.DataSource;

import io.cockroachdb.dlr.core.model.Table;
import io.cockroachdb.dlr.pubsub.Publisher;

public interface AsyncProducer {
    void initialize(DataSource dataSource, Publisher publisher, Table table);
}
