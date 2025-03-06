package io.cockroachdb.volt.util.wgs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import io.cockroachdb.volt.util.wgs.Latitude;
import io.cockroachdb.volt.util.wgs.Longitude;
import io.cockroachdb.volt.util.wgs.Position;

public class PositionTest {
    @Test
    public void testLatitudeToFromDMS() {
        IntStream.rangeClosed(0, 90 - 1).forEach(
                value -> IntStream.rangeClosed(2, 60).forEach(f -> {
                    double fraction = 1.0 / f;
                    Latitude before = Latitude.fromDecimal(value + fraction);
                    Latitude after = Latitude.fromDMS(before.toDMS());
                    Assertions.assertEquals(before, after);
                }));
        IntStream.rangeClosed(-90, 0).forEach(
                value -> IntStream.rangeClosed(2, 60).forEach(f -> {
                    double fraction = 1.0 / f;
                    Latitude before = Latitude.fromDecimal(value + fraction);
                    Latitude after = Latitude.fromDMS(before.toDMS());
                    Assertions.assertEquals(before, after);
                }));
    }

    @Test
    public void testLongitudeToFromDMS() {
        IntStream.rangeClosed(0, 180 - 1).forEach(
                value -> IntStream.rangeClosed(2, 60).forEach(f -> {
                    double fraction = 1.0 / f;
                    Longitude before = Longitude.fromDecimal(value + fraction);
                    Longitude after = Longitude.fromDMS(before.toDMS());
                    Assertions.assertEquals(before, after);
                }));
        IntStream.rangeClosed(-180, 0).forEach(
                value -> IntStream.rangeClosed(2, 60).forEach(f -> {
                    double fraction = 1.0 / f;
                    Longitude before = Longitude.fromDecimal(value + fraction);
                    Longitude after = Longitude.fromDMS(before.toDMS());
                    Assertions.assertEquals(before, after);
                }));
    }

    @Test
    public void testPositions() {
        Position p1 = Position.of(Latitude.fromDecimal(59), Longitude.fromDecimal(18));
        Position p2 = Position.of(Latitude.fromDecimal(49), Longitude.fromDecimal(0));

        double d = Position.haversineDistance(p1, p2);
        double b = Position.bearing(p1, p2);

        System.out.println(p1.toDMS());
        System.out.println(p2.toDMS());
        System.out.println(d);
        System.out.println(b);
    }
}
