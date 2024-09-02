package io.roach.volt.util.pubsub;

@FunctionalInterface
public interface MessageListener<E> {
    void onMessage(Message<E> message);
}
