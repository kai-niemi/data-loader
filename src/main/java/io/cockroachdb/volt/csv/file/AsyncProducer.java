package io.cockroachdb.volt.csv.file;

import javax.sql.DataSource;

import io.cockroachdb.volt.csv.model.Table;
import io.cockroachdb.volt.pubsub.Publisher;

public interface AsyncProducer {
    void initialize(DataSource dataSource, Publisher publisher, Table table);
}
