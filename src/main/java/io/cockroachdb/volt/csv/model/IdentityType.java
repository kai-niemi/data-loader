package io.cockroachdb.volt.csv.model;

public enum IdentityType {
    sequence,
    database_sequence,
    ordered,
    unordered,
    uuid
}
