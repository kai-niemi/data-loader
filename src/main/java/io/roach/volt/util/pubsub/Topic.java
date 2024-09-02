package io.roach.volt.util.pubsub;

public interface Topic<E> {
    void addMessageListener(MessageListener<E> listener);

    void publish(E message);

    void publishAsync(E message);
}
