package io.cockroachdb.dl.util.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CircularFifoQueue<K, V> implements FifoQueue<K, V> {
    private final Map<String, RingBuffer<Map<K, V>>> ringBuffers
            = new ConcurrentHashMap<>();

    private final int bufferCapacity;

    public CircularFifoQueue(int bufferCapacity) {
        if (bufferCapacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.bufferCapacity = bufferCapacity;
    }

    private RingBuffer<Map<K, V>> ringBufferFor(String key) {
        ringBuffers.putIfAbsent(key, new RingBuffer<>(bufferCapacity));
        return ringBuffers.get(key);
    }

    @Override
    public Map<K, V> take(String key) throws InterruptedException {
        RingBuffer<Map<K, V>> ringBuffer = ringBufferFor(key);
        Map<K, V> values = ringBuffer.getRandom();
        while (values == null) {
            TimeUnit.MILLISECONDS.sleep(500);
            values = ringBuffer.getRandom();
        }
        return values;
    }

    @Override
    public void put(String key, Map<K, V> values) {
        ringBufferFor(key).add(ConcurrencyUtils.immutableCopyOf(values));
    }
}
