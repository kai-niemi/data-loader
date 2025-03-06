package io.cockroachdb.volt.csv.file;

import java.util.function.Supplier;

/**
 * Producer side for generated single insertion-ordered key-value pairs.
 *
 * @param <K> map key type
 * @param <V> map key value
 */
//@FunctionalInterface
public interface ChunkProducer<K, V> {
    /**
     * Produce key-value chunks and supply to consumer until consumer tells
     * to stop.
     *
     * @param consumer the target consumer
     * @throws Exception on any errors
     */
    void produceChunks(ChunkConsumer<K, V> consumer) throws Exception;

    Supplier<Integer> currentRow();
}
