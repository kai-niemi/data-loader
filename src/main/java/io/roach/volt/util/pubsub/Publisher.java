package io.roach.volt.util.pubsub;

import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.GenericEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.lang.ref.WeakReference;
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

/**
 * A basic publish/subscribe orchestrator using blocking queues and parallelism.
 */
@Component
public class Publisher {
    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);

    @Autowired
    @Qualifier("pubSubExecutorService")
    private ExecutorService executorService;

    @Value("${application.queue-size}")
    private int queueSize;

    private final Map<String, Topic<?>> topics = new ConcurrentHashMap<>();

    private final List<WeakReference<Future<?>>> futures = new CopyOnWriteArrayList<>();

    @SuppressWarnings("unchecked")
    public <E> Topic<E> getTopic(String name) {
        return (Topic<E>) topics.computeIfAbsent(name, n -> {
            SimpleTopic<E> topic = new SimpleTopic<>(n, queueSize);
            Future<?> f = executorService.submit(topic::drainAndSend);

            futures.add(new WeakReference<>(f));

            logger.trace("Created topic '%s' with queue size %d"
                    .formatted(n, queueSize));

            return topic;
        });
    }

    @EventListener
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) {
        futures.forEach(futureWeakReference -> {
            Future<?> f = futureWeakReference.get();
            if (f != null) {
                f.cancel(true);
            }
            futureWeakReference.clear();
        });

        topics.clear();
        futures.clear();

        logger.trace("Cancelled all futures");
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
            logger.trace("Started draining topic '%s'".formatted(name));

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Message<E> message = Message.of(queue.take())
                            .setTopic(name);

                    eventsDequeued.incrementAndGet();

                    listeners.parallelStream()
                            .forEach(listener -> {
                                eventsPublished.incrementAndGet();
                                listener.onMessage(message);
                            });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                logger.trace(("Finished draining topic '%s' - queued: %,d dequeued: %,d published: %,d")
                        .formatted(name,
                                eventsQueued.get(),
                                eventsDequeued.get(),
                                eventsPublished.get()));
            }
        }

        @Override
        public void addMessageListener(MessageListener<E> listener) {
            logger.trace("Add message listener for topic '%s': %s".formatted(name, listener));
            listeners.add(listener);
        }

        @Override
        public void removeMessageListener(MessageListener<E> listener) {
            logger.trace("Remove message listener from topic '%s': %s".formatted(name, listener));
            listeners.remove(listener);
        }

        @Override
        public void publish(E payload) {
            Message<E> message = Message.of(payload)
                    .setTopic(name);

            listeners.parallelStream()
                    .forEach(listener -> {
                        eventsPublished.incrementAndGet();
                        listener.onMessage(message);
                    });
        }

        @Override
        public void publishAsync(E payload) {
            try {
                eventsQueued.incrementAndGet();
                queue.put(payload);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException();
            }
        }
    }
}
