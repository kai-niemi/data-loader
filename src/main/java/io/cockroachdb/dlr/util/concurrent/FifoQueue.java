package io.cockroachdb.dlr.util.concurrent;

import java.util.Map;

public interface FifoQueue<K, V> {
    void put(String key, Map<K, V> values) throws InterruptedException;

    Map<K, V> take(String key) throws InterruptedException;
}
