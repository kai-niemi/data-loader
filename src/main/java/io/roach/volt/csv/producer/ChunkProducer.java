package io.roach.volt.csv.producer;

@FunctionalInterface
public interface ChunkProducer<K, V> {
    void produceChunks(ChunkConsumer<K, V> consumer) throws Exception;
}
