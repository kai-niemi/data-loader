package io.cockroachdb.volt.util;

import org.junit.jupiter.api.Test;

import io.cockroachdb.volt.util.RandomData;

public class RandomDataTest {
    @Test
    public void randomJson() {
        System.out.println(RandomData.randomJson(2, 2));
    }
}
