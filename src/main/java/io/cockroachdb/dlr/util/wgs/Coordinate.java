package io.cockroachdb.dlr.util.wgs;

public interface Coordinate {
    double getDegrees();

    String toDMS();
}
