package io.roach.volt.csv.generator;

import java.util.UUID;

public class UUIDGenerator implements ColumnGenerator<UUID> {
    @Override
    public UUID nextValue() {
        return UUID.randomUUID();
    }
}
