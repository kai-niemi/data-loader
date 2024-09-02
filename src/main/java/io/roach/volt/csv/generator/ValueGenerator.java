package io.roach.volt.csv.generator;

public interface ValueGenerator<T> {
    T nextValue();
}
