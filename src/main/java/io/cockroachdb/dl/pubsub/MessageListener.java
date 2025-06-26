package io.cockroachdb.dl.pubsub;

@FunctionalInterface
public interface MessageListener<E> {
    void onMessage(Message<E> message);
}
