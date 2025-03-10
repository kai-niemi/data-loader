package io.cockroachdb.dlr.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Support utility for computing the cartesian products for N sets (A x B x C ..).
 *
 * @author Kai Niemi
 */
public abstract class Cartesian {
    private Cartesian() {
    }

    public static <T> List<List<T>> cartesianProductList(List<List<T>> sets) {
        return cartesianProductStream(sets, 0)
                .collect(Collectors.toList());
    }

    public static <T> Stream<List<T>> cartesianProductStream(List<List<T>> sets) {
        return cartesianProductStream(sets, 0);
    }

    private static <T> Stream<List<T>> cartesianProductStream(List<List<T>> sets, int index) {
        if (index == sets.size()) {
            return Stream.of(List.of()); // terminator
        }
        return sets.get(index).stream()
                .flatMap(element -> cartesianProductStream(sets, index + 1)
                        .map(list -> {
                            List<T> newList = new ArrayList<>(list);
                            newList.add(0, element);
                            return newList;
                        }));
    }

}
