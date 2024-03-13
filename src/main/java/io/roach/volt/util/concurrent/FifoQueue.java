package io.roach.volt.util.concurrent;

import java.util.Map;

public interface FifoQueue<K, V> {
    Map<K, V> take(String key);

    Map<K, V> selectRandom(String key);

    void put(String key, Map<K, V> values);

    void offer(String key, Map<K, V> values);
}
