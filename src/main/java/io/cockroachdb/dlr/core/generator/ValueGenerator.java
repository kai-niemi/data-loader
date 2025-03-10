package io.cockroachdb.dlr.core.generator;

public interface ValueGenerator<T> {
    T nextValue();
}
