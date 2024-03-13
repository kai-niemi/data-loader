package io.roach.volt.csv.producer;

import io.roach.volt.util.pubsub.Publisher;

@FunctionalInterface
public interface ChunkProducer<K, V> {
    void produce(Publisher publisher, ChunkConsumer<K, V> consumer);
}
