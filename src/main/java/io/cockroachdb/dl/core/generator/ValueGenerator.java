package io.cockroachdb.dl.core.generator;

public interface ValueGenerator<T> {
    T nextValue();
}
