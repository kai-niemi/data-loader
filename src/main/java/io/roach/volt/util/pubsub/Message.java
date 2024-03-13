package io.roach.volt.util.pubsub;

import org.springframework.util.Assert;

import java.time.Instant;

public class Message<E> {
    public static <E> Message<E> of(E object) {
        return new Message<>(object, Instant.now());
    }

    private final E payload;

    private final Instant publishTime;

    private String topic;

    private Message(E payload, Instant publishTime) {
        Assert.notNull(payload, "payload is null");
        Assert.notNull(publishTime, "publishTime is null");
        this.payload = payload;
        this.publishTime = publishTime;
    }

    public Message<E> setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public E getPayload() {
        return payload;
    }

    public Instant getPublishTime() {
        return publishTime;
    }
}
