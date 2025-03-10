package io.cockroachdb.dlr.pubsub;

@FunctionalInterface
public interface MessageListener<E> {
    void onMessage(Message<E> message);
}
