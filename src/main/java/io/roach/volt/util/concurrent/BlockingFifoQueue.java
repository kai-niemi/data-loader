package io.roach.volt.util.concurrent;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class BlockingFifoQueue<K, V> implements FifoQueue<K, V> {
    private final Map<String, BlockingQueue<Map<K, V>>> blockingQueues
            = new ConcurrentHashMap<>();

    private final int bufferCapacity;

    public BlockingFifoQueue(int bufferCapacity) {
        if (bufferCapacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.bufferCapacity = bufferCapacity;
    }

    private BlockingQueue<Map<K, V>> queueFor(String key) {
        blockingQueues.putIfAbsent(key, new LinkedBlockingDeque<>(bufferCapacity));
        return blockingQueues.get(key);
    }

    @Override
    public Map<K, V> take(String key) {
        try {
            return queueFor(key).take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UndeclaredThrowableException(e, "Interrupted take for key: " + key);
        }
    }

    @Override
    public void put(String key, Map<K, V> values) {
        try {
            queueFor(key).put(ConcurrencyUtils.immutableCopyOf(values));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UndeclaredThrowableException(e, "Interrupted put for key " + key);
        }
    }
}

