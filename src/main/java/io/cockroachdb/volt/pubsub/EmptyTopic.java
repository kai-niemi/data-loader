package io.cockroachdb.volt.pubsub;

public class EmptyTopic<E> implements Topic<E> {
    @Override
    public void addMessageListener(MessageListener<E> listener) {

    }

    @Override
    public boolean hasMessageListeners() {
        return false;
    }

    @Override
    public void publish(Message<E> message) {

    }
}
