package io.cockroachdb.volt.util.wgs;

public interface Coordinate {
    double getDegrees();

    String toDMS();
}
