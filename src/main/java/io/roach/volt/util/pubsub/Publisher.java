package io.roach.volt.util.pubsub;

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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.GenericEvent;

/**
 * A basic publish/subscribe orchestrator using blocking queues and parallelism.
 */
@Component
public class Publisher {
    public static final int DEFAULT_QUEUE_SIZE = 8192;

    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);

    @Autowired
    @Qualifier("pubSubExecutorService")
    private ExecutorService executorService;

    private final Map<String, Topic<?>> topics = new ConcurrentHashMap<>();

    private final List<WeakReference<Future<?>>> futures = new CopyOnWriteArrayList<>();

    @SuppressWarnings("unchecked")
    public <E> Topic<E> getTopic(String name) {
        return (Topic<E>) topics.computeIfAbsent(name, n -> {
            SimpleTopic<E> topic = new SimpleTopic<>(n, DEFAULT_QUEUE_SIZE);

            Future<?> f = executorService.submit(topic::drainAndSend);

            futures.add(new WeakReference<>(f));

            logger.trace("Created topic '%s' with queue size %d"
                    .formatted(n, DEFAULT_QUEUE_SIZE));

            return topic;
        });
    }

    @EventListener
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) {
        logger.debug("Cancelling futures");

        futures.forEach(futureWeakReference -> {
            Future<?> f = futureWeakReference.get();
            if (f != null) {
                f.cancel(true);
            }
            futureWeakReference.clear();
        });
        topics.clear();
        futures.clear();

        logger.debug("Cancelled all futures");
    }

    private static class SimpleTopic<E> implements Topic<E> {
        private final String name;

        private final AtomicInteger eventsPublished = new AtomicInteger();

        private final AtomicInteger eventsQueued = new AtomicInteger();

        private final AtomicInteger eventsDequeued = new AtomicInteger();

        private final List<MessageListener<E>> listeners = new LinkedList<>();

        private final BlockingDeque<E> queue;

        private SimpleTopic(String name, int queueSize) {
            this.name = name;
            this.queue = new LinkedBlockingDeque<>(queueSize);
        }

        private void drainAndSend() {
            listeners.forEach(e -> {
                logger.debug("Started draining topic '%s' for listener: %s"
                        .formatted(name, e.toString()));
            });

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    publish(queue.take());
                    eventsDequeued.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UndeclaredThrowableException(e);
            } finally {
                logger.debug(("Finished draining topic '%s' - queued: %d dequeued: %d published: %d")
                        .formatted(name,
                                eventsQueued.get(),
                                eventsDequeued.get(),
                                eventsPublished.get()));
            }
        }

        @Override
        public void addMessageListener(MessageListener<E> listener) {
            logger.trace("Add message listener for topic '%s': %s"
                    .formatted(name, listener.toString()));
            listeners.add(listener);
        }

        @Override
        public void publish(E payload) {
            Message<E> message = Message.of(payload)
                    .setTopic(name);

            listeners.parallelStream()
                    .forEach(listener -> {
                        logger.debug("Publish payload for topic '%s' listener '%s':\n  %s"
                                .formatted(name, listener.toString(), payload));
                        listener.onMessage(message);
                    });

            eventsPublished.incrementAndGet();
        }

        @Override
        public void publishAsync(E payload) {
            try {
                queue.put(payload);
                eventsQueued.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException();
            }
        }
    }
}
