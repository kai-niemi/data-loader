package io.roach.volt.util.pubsub;

public interface Topic<E> {
    class Empty<E> implements Topic<E> {
        @Override
        public void addMessageListener(MessageListener<E> listener) {

        }

        @Override
        public boolean hasMessageListeners() {
            return false;
        }

        @Override
        public void publish(E message) {

        }
    }

    void addMessageListener(MessageListener<E> listener);

    boolean hasMessageListeners();

    void publish(E message);
}
