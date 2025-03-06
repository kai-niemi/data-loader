package io.cockroachdb.volt.util.concurrent;

import java.util.concurrent.ThreadLocalRandom;

@NotThreadSafe
public class RingBuffer<T> {
    private int index;

    private final int size;

    private final T[] buffer;

    public RingBuffer(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be > 0");
        }
        this.size = size;
        //noinspection unchecked
        this.buffer = (T[]) new Object[this.size];
    }

    public void add(T item) {
        this.buffer[this.index] = item;
        this.index = (this.index + 1) % this.size;
    }

    public T get(int i) {
        if (i < 0) {
            throw new IllegalArgumentException("Index must be >= 0");
        }
        return this.buffer[i % this.size];
    }

    public T getRandom() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int bound = Math.min(size, index);
        return bound > 0 ? buffer[random.nextInt(bound)] : null;
    }
}