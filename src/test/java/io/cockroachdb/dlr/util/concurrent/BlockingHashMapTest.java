package io.cockroachdb.dlr.util.concurrent;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BlockingHashMapTest {
    @Test
    public void testBlockingMap() throws InterruptedException {
        BlockingHashMap<String, Integer> map = new BlockingHashMap<>();

        map.put("a", 1);
        map.put("a", 2);
        map.put("a", 3);

        CompletableFuture<Integer> f1 = CompletableFuture.supplyAsync(() -> {
            try {
                return map.get("a");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        CompletableFuture<Integer> f2 = CompletableFuture.supplyAsync(() -> {
            try {
                return map.get("a");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        CompletableFuture<Integer> f3 = CompletableFuture.supplyAsync(() -> {
            try {
                return map.get("a");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Assertions.assertEquals(3, f1.join());
        Assertions.assertEquals(3, f2.join());
        Assertions.assertEquals(3, f3.join());
    }
}
