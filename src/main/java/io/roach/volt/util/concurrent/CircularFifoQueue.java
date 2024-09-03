package io.roach.volt.util.concurrent;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

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

    private BlockingQueue<Map<K, V>> queueForKey(String key) {
        blockingQueues.putIfAbsent(key, new LinkedBlockingDeque<>(bufferCapacity));
        return blockingQueues.get(key);
    }

    private RingBuffer<Map<K, V>> bufferForKey(String key) {
        ringBuffers.putIfAbsent(key, new RingBuffer<>(bufferCapacity));
        return ringBuffers.get(key);
    }

    @Override
    public Map<K, V> take(String key) {
        try {
            BlockingQueue<Map<K, V>> q = queueForKey(key);
            return q.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UndeclaredThrowableException(e, "Interrupted take for key: " + key);
        }
    }

    @Override
    public Map<K, V> peekRandom(String key) {
        RingBuffer<Map<K, V>> ringBuffer = bufferForKey(key);
        Map<K, V> values = ringBuffer.getRandom();
        while (values == null) {
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UndeclaredThrowableException(e, "Interrupted peekRandom for key: " + key);
            }
            values = ringBuffer.getRandom();
        }
        return values;
    }

    @Override
    public void put(String key, Map<K, V> values) {
        try {
            Map<K, V> copy = immutableCopyOf(values);
            queueForKey(key).put(copy);
            bufferForKey(key).add(copy);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UndeclaredThrowableException(e, "Interrupted put for key " + key);
        }
    }

    @Override
    public void offer(String key, Map<K, V> values) {
        Map<K, V> copy = immutableCopyOf(values);
        bufferForKey(key).add(copy);
    }

    private Map<K, V> immutableCopyOf(Map<K, V> values) {
        try {
            return Map.copyOf(values);
        } catch (NullPointerException e) {
            K k = values
                    .entrySet()
                    .stream()
                    .filter(kvEntry -> kvEntry.getValue() == null)
                    .findAny()
                    .orElseThrow(
                            () -> new UndeclaredThrowableException(e, "Collection was null or contained null values!"))
                    .getKey();
            throw new UndeclaredThrowableException(e, "Collection contained null value for key " + k);
        }
    }
}
