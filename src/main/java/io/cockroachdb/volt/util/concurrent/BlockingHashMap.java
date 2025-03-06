package io.cockroachdb.volt.util.concurrent;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingHashMap<K, V> {
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<K, BlockingQueue<V>> concurrentHashMap = new ConcurrentHashMap<>();

    private BlockingQueue<V> getQueue(K key) {
        return concurrentHashMap.computeIfAbsent(key, k -> new ArrayBlockingQueue<>(1));
    }

    public void put(K key, V value) throws InterruptedException {
        try {
            lock.lock();
            BlockingQueue<V> q = getQueue(key);
            while (!q.offer(value)) {
                q.poll();
            }
        } finally {
            lock.unlock();
        }
    }

    public V get(K key) throws InterruptedException {
        try {
            lock.lock();
            BlockingQueue<V> q = getQueue(key);
            V v = q.take();
            q.put(v);
            return v;
        } finally {
            lock.unlock();
        }
    }

    public V get(K key, long timeout, TimeUnit unit) throws InterruptedException {
        return getQueue(key).poll(timeout, unit);
    }
}
