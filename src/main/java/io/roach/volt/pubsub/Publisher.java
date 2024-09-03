package io.roach.volt.pubsub;

import java.lang.ref.WeakReference;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import io.roach.volt.csv.event.CancellationEvent;
import io.roach.volt.csv.event.GenericEvent;

/**
 * A basic publish/subscribe orchestrator using blocking queues and parallelism.
 */
@Component
public class Publisher {
    public static final int DEFAULT_QUEUE_SIZE = 8192;

    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);

    private final List<WeakReference<Future<?>>> futures = new CopyOnWriteArrayList<>();

    private final Map<String, Topic<?>> topics = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("pubSubExecutorService")
    private ExecutorService executorService;

    private int queueSize = DEFAULT_QUEUE_SIZE;

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    @SuppressWarnings("unchecked")
    public <E> Topic<E> getTopic(String name) {
        return (Topic<E>) topics.computeIfAbsent(name, n -> {
            DefaultTopic<E> topic = new DefaultTopic<>(name, queueSize);

            Future<?> future = executorService.submit(() -> {
                try {
                    topic.drainAndBroadcast();
                } catch (Throwable e) {
                    logger.error("Error draining topic [%s]".formatted(name), e);
                    throw new UndeclaredThrowableException(e);
                } finally {
                    topics.remove(name);
                    logger.info("Removed topic [%s]".formatted(name));
                }
            });

            futures.add(new WeakReference<>(future));

            logger.info("Created topic [%s] with queue size %d".formatted(name, queueSize));

            return topic;
        });
    }

    public void cancel() {
        futures.forEach(futureWeakReference -> {
            Future<?> f = futureWeakReference.get();
            if (f != null) {
                logger.warn("Cancelling future: %s".formatted(f));
                f.cancel(true);
            }
            futureWeakReference.clear();
        });

        logger.info("Cancelled %d futures".formatted(futures.size()));

        futures.clear();
    }

    @EventListener
    public void onCancelEvent(GenericEvent<CancellationEvent> event) {
        logger.info("Cancellation request received");
        cancel();
    }

    private static class DefaultTopic<E> implements Topic<E> {
        private final String name;

        private final AtomicInteger eventsPublished = new AtomicInteger();

        private final AtomicInteger eventsQueued = new AtomicInteger();

        private final AtomicInteger eventsDequeued = new AtomicInteger();

        private final List<MessageListener<E>> listeners = new LinkedList<>();

        private final BlockingDeque<Message<E>> queue;

        private final Semaphore drainSemaphore = new Semaphore(0);

        private DefaultTopic(String name, int queueSize) {
            this.name = name;
            this.queue = new LinkedBlockingDeque<>(queueSize);
        }

        private void drainAndBroadcast() {
            try {
                logger.debug("Draining queued to start for topic [%s]".formatted(name));
                drainSemaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            try {
                logger.debug("Draining topic [%s] with %d listeners".formatted(name, listeners.size()));

                while (!Thread.currentThread().isInterrupted()) {
                    Message<E> message = queue.take();

                    Assert.notEmpty(listeners, "no listeners");
                    listeners.parallelStream().forEach(listener -> listener.onMessage(message));

                    eventsDequeued.incrementAndGet();
                    eventsPublished.incrementAndGet();

                    if (message.isPoisonPill()) {
                        logger.debug("Closing topic [%s] due to poison pill".formatted(name));
                        break;
                    }
                }

                logger.debug(("Drained out topic [%s] - queued(%d) dequeued(%d) published(%d)")
                        .formatted(name,
                                eventsQueued.get(),
                                eventsDequeued.get(),
                                eventsPublished.get()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted topic [%s]".formatted(name));
            }
        }

        @Override
        public void addMessageListener(MessageListener<E> listener) {
            listeners.add(listener);
            drainSemaphore.release();
        }

        @Override
        public boolean hasMessageListeners() {
            return !listeners.isEmpty();
        }

        @Override
        public void publish(Message<E> message) {
            Assert.notEmpty(listeners, "no listeners");
            try {
                queue.put(message);
                eventsQueued.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted publish to topic: " + name, e);
            }
        }
    }
}
