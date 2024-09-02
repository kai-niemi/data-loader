package io.roach.volt.util.concurrent;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class CircularFifoQueue<K, V> implements FifoQueue<K, V> {
    private final Map<String, BlockingQueue<Map<K, V>>> blockingQueues
            = new ConcurrentHashMap<>();

    private final Map<String, RingBuffer<Map<K, V>>> ringBuffers
            = new ConcurrentHashMap<>();

    private final int bufferCapacity;

    public CircularFifoQueue(int bufferCapacity) {
        if (bufferCapacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.bufferCapacity = bufferCapacity;
    }

    private BlockingQueue<Map<K, V>> getOrCreateQueueForKey(String key) {
        blockingQueues.putIfAbsent(key, new LinkedBlockingDeque<>(bufferCapacity));
        return blockingQueues.get(key);
    }

    private RingBuffer<Map<K, V>> getOrCreateBufferForKey(String key) {
        ringBuffers.putIfAbsent(key, new RingBuffer<>(bufferCapacity));
        return ringBuffers.get(key);
    }

    @Override
    public Map<K, V> take(String key) {
        BlockingQueue<Map<K, V>> q = getOrCreateQueueForKey(key);
        try {
            return q.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UndeclaredThrowableException(e, "Interrupted take for key: " + key);
        }
    }

    @Override
    public Map<K, V> selectRandom(String key) {
        Map<K, V> values = getOrCreateBufferForKey(key).getRandom();
        if (values == null) {
            values = take(key);
            put(key, values);
        }
        return values;
    }

    private Map<K, V> immutableCopyOf(Map<K, V> values) {
        return Map.copyOf(values);
    }

    @Override
    public void put(String key, Map<K, V> values) {
        try {
            Map<K, V> copy = immutableCopyOf(values);
            getOrCreateQueueForKey(key)
                    .put(copy);
            getOrCreateBufferForKey(key)
                    .add(copy);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UndeclaredThrowableException(e, "Interrupted put for key " + key);
        }
    }

    @Override
    public void offer(String key, Map<K, V> values) {
        Map<K, V> copy = immutableCopyOf(values);
        BlockingQueue<Map<K, V>> queueForKey = getOrCreateQueueForKey(key);
        while (!queueForKey.offer(copy)) {
            queueForKey.poll();
        }
        getOrCreateBufferForKey(key).add(copy);
    }
}
