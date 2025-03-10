package io.cockroachdb.dlr.util.concurrent;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class ConcurrencyUtils {
    private ConcurrencyUtils() {
    }

    public static <K,V> Map<K, V> immutableCopyOf(Map<K, V> values) {
        try {
            return Map.copyOf(values);
        } catch (NullPointerException e) {
            K k = values
                    .entrySet()
                    .stream()
                    .filter(kvEntry -> kvEntry.getValue() == null)
                    .findAny()
                    .orElseThrow(
                            () -> new UndeclaredThrowableException(e, "Collection was null or contained null values!"))
                    .getKey();
            throw new IllegalArgumentException("Map value is null for key " + k);
        }
    }

    public static <T> CompletableFuture<List<T>> joinAndMapResults(List<CompletableFuture<T>> futures) {
        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }
}
