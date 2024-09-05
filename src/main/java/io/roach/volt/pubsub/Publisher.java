package io.roach.volt.pubsub;

import java.lang.ref.WeakReference;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import io.roach.volt.csv.event.CancellationEvent;
import io.roach.volt.csv.event.GenericEvent;

/**
 * A basic publish/subscribe orchestrator using blocking queues and parallelism.
 */
@Component
public class Publisher {
    public static final int DEFAULT_QUEUE_SIZE = 8192;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<WeakReference<Future<Void>>> futures = new CopyOnWriteArrayList<>();

    private final Map<String, Topic<?>> topics = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("asyncTaskExecutor")
    private ThreadPoolTaskExecutor threadPoolExecutor;

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

            Future<Void> future = threadPoolExecutor.submit(() -> {
                try {
                    DefaultTopic.Stats stats = topic.drainAndBroadcast();
                    logger.info("Topic [%s] drained - listeners(%d) queued(%d) dequeued(%d) published(%d)"
                            .formatted(stats.getName(),
                                    stats.getListeners(),
                                    stats.getEventsQueued(),
                                    stats.getEventsDequeued(),
                                    stats.getEventsPublished()));
                    return null;
                } catch (Throwable e) {
                    logger.error("Uncategorized error draining topic [%s]".formatted(name), e);
                    throw new UndeclaredThrowableException(e);
                } finally {
                    topics.remove(name);
                    logger.info("Topic [%s] removed - remaining: %s"
                            .formatted(name, topics));
                }
            });

            futures.add(new WeakReference<>(future));

            logger.info("Topic [%s] created - queue size %d".formatted(name, queueSize));

            return topic;
        });
    }

    public void cancel() {
        futures.forEach(futureWeakReference -> {
            try {
                Future<Void> f = futureWeakReference.get();
                if (f != null) {
                    logger.warn("Cancelling future: %s".formatted(f));
                    f.cancel(true);
                }
            } finally {
                futureWeakReference.clear();
            }
        });

        logger.info("Cancelled %d futures".formatted(futures.size()));

        futures.clear();
    }

    @EventListener
    public void onCancelEvent(GenericEvent<CancellationEvent> event) {
        cancel();
    }
}
