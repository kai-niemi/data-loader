package io.cockroachdb.dl.core;

import javax.sql.DataSource;

import io.cockroachdb.dl.core.model.Table;
import io.cockroachdb.dl.pubsub.Publisher;

public interface AsyncProducer {
    void initialize(DataSource dataSource, Publisher publisher, Table table);
}
