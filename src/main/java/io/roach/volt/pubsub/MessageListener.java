package io.roach.volt.pubsub;

@FunctionalInterface
public interface MessageListener<E> {
    void onMessage(Message<E> message);
}
