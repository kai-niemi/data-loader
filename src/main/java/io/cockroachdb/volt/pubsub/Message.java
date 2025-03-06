package io.cockroachdb.volt.pubsub;

import java.time.Instant;

import org.springframework.util.Assert;

public class Message<E> {
    public static <E> Message<E> of(E object) {
        return new Message<>(Instant.now(),object);
    }

    public static <E> Message<E> poisonPill() {
        return new Message<>(Instant.now());
    }

    private final E payload;

    private final Instant publishTime;

    private final boolean poisonPill;

    private Message(Instant publishTime, E payload) {
        Assert.notNull(publishTime, "publishTime is null");
        Assert.notNull(payload, "payload is null");
        this.payload = payload;
        this.publishTime = publishTime;
        this.poisonPill = false;
    }

    private Message(Instant publishTime) {
        Assert.notNull(publishTime, "publishTime is null");
        this.payload = null;
        this.publishTime = publishTime;
        this.poisonPill = true;
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }

    public E getPayload() {
        return payload;
    }

    public Instant getPublishTime() {
        return publishTime;
    }

    @Override
    public String toString() {
        return "Message{" +
                "payload=" + payload +
                ", publishTime=" + publishTime +
                ", poisonPill=" + poisonPill +
                '}';
    }
}
