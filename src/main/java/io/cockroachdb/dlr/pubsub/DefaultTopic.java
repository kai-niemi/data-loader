package io.cockroachdb.dlr.pubsub;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

public class DefaultTopic<E> implements Topic<E> {
    public static class Stats {
        private final String name;

        private final AtomicInteger eventsPublished = new AtomicInteger();

        private final AtomicInteger eventsQueued = new AtomicInteger();

        private final AtomicInteger eventsDequeued = new AtomicInteger();

        private final AtomicInteger listeners = new AtomicInteger();

        public Stats(String name) {
            this.name = name;
        }

        public int getEventsDequeued() {
            return eventsDequeued.get();
        }

        public int getEventsPublished() {
            return eventsPublished.get();
        }

        public int getEventsQueued() {
            return eventsQueued.get();
        }

        public int getListeners() {
            return listeners.get();
        }

        public String getName() {
            return name;
        }
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Stats stats;

    private final String name;

    private final List<MessageListener<E>> listeners = new LinkedList<>();

    private final BlockingDeque<Message<E>> queue;

    private final Semaphore semaphore = new Semaphore(0);

    public DefaultTopic(String name, int queueSize) {
        this.name = name;
        this.queue = new LinkedBlockingDeque<>(queueSize);
        this.stats = new Stats(name);
    }

    public Stats drainAndBroadcast() {
        try {
            logger.debug("Draining topic [%s] - waiting for semaphore".formatted(name));
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            logger.debug("Draining topic [%s] with %d listeners".formatted(name, listeners.size()));

            while (!Thread.currentThread().isInterrupted()) {
                Message<E> message = queue.take();

                Assert.notEmpty(listeners, "no listeners");
                listeners.parallelStream().forEach(listener -> listener.onMessage(message));

                stats.eventsDequeued.incrementAndGet();
                stats.eventsPublished.incrementAndGet();

                if (message.isPoisonPill()) {
                    logger.debug("Suspending draining topic [%s] due to poison pill".formatted(name));
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted topic [%s]".formatted(name));
        }

        return stats;
    }

    @Override
    public void addMessageListener(MessageListener<E> listener) {
        listeners.add(listener);
        stats.listeners.incrementAndGet();
        // Release queue drain and message broadcast semaphore
        semaphore.release();
    }

    @Override
    public boolean hasMessageListeners() {
        return !listeners.isEmpty();
    }

    @Override
    public void publish(Message<E> message) {
        if (listeners.isEmpty()) {
            logger.warn("No registered listeners - queue may fill up and block");
        }
        try {
            queue.put(message);
            stats.eventsQueued.incrementAndGet();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted publish for topic: " + name, e);
        }
    }
}
