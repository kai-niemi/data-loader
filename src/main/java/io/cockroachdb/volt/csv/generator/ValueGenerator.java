package io.cockroachdb.volt.csv.generator;

public interface ValueGenerator<T> {
    T nextValue();
}
