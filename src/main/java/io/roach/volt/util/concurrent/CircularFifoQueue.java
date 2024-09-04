package io.roach.volt.util.concurrent;

import java.lang.reflect.UndeclaredThrowableException;
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
    public Map<K, V> take(String key) {
        RingBuffer<Map<K, V>> ringBuffer = ringBufferFor(key);
        Map<K, V> values = ringBuffer.getRandom();
        while (values == null) {
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UndeclaredThrowableException(e, "Interrupted take for key: " + key);
            }
            values = ringBuffer.getRandom();
        }
        return values;
    }

    @Override
    public void put(String key, Map<K, V> values) {
        ringBufferFor(key).add(ConcurrencyUtils.immutableCopyOf(values));
    }
}
