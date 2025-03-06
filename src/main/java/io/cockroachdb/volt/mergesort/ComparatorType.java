package io.cockroachdb.volt.mergesort;

import java.util.Comparator;
import java.util.List;

public enum ComparatorType {
    auto {
        @Override
        public Comparator<String> comparator(String delimiter, List<Integer> orderBy) {
            if (orderBy.isEmpty()) {
                return Comparator.naturalOrder();
            }
            return fast.comparator(delimiter, orderBy);
        }
    },
    strict {
        @Override
        public Comparator<String> comparator(String delimiter, List<Integer> orderBy) {
            return new StrictComparator(delimiter, orderBy);
        }
    },
    fast {
        @Override
        public Comparator<String> comparator(String delimiter, List<Integer> orderBy) {
            return new FastComparator(delimiter, orderBy);
        }
    },
    simple {
        @Override
        public Comparator<String> comparator(String delimiter, List<Integer> orderBy) {
            return new FastComparator(delimiter, orderBy);
        }
    };

    public abstract Comparator<String> comparator(String delimiter, List<Integer> orderBy);
}
