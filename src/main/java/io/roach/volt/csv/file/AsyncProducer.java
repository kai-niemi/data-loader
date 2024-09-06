package io.roach.volt.csv.file;

import javax.sql.DataSource;

import io.roach.volt.csv.model.Table;
import io.roach.volt.pubsub.Publisher;

public interface AsyncProducer {
    void initialize(DataSource dataSource, Publisher publisher, Table table);
}
