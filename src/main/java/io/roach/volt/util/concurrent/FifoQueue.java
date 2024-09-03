package io.roach.volt.util.concurrent;

import java.util.Map;

public interface FifoQueue<K, V> {
    void put(String key, Map<K, V> values);

    Map<K, V> take(String key);

    void offer(String key, Map<K, V> values);

    Map<K, V> peekRandom(String key);
}
