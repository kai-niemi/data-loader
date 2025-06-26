package io.cockroachdb.dl.util.wgs;

public interface Coordinate {
    double getDegrees();

    String toDMS();
}
