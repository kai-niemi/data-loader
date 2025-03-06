package io.cockroachdb.volt.util.concurrent;

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
    public Map<K, V> take(String key) throws InterruptedException {
        return queueFor(key).take();
    }

    @Override
    public void put(String key, Map<K, V> values) throws InterruptedException {
        queueFor(key).put(ConcurrencyUtils.immutableCopyOf(values));
    }
}

