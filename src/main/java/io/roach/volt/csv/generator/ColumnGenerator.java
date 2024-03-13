package io.roach.volt.csv.generator;

public interface ColumnGenerator<T> {
    T nextValue();
}
