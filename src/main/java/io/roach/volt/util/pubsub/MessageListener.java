package io.roach.volt.util.pubsub;

@FunctionalInterface
public interface MessageListener<E> {
//    String getName();
    void onMessage(Message<E> message);
}
