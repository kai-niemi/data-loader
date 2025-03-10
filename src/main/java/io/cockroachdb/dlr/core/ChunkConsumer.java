package io.cockroachdb.dlr.core;

import java.util.Map;

/**
 * Consumer side of generated single insertion-ordered key-value pairs.
 *
 * @param <K> map key type
 * @param <V> map key value
 */
@FunctionalInterface
public interface ChunkConsumer<K,V> {
    /**
     * Consume a single insertion-ordered key-value pair of generated values.
     *
     * @param values      single row tuple
     * @param rowEstimate estimated total number of rows
     * @return true to signal continuation
     */
    boolean consumeChunk(Map<K,V> values, long rowEstimate) throws Exception;
}
