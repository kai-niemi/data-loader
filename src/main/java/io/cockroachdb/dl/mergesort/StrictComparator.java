package io.cockroachdb.dl.mergesort;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.LineTokenizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class StrictComparator implements Comparator<String> {
    private final LineTokenizer tokenizer;

    private final List<Integer> orderBy;

    public StrictComparator(String delimiter, List<Integer> orderBy) {
        this.tokenizer = new DelimitedLineTokenizer(delimiter);
        this.orderBy = orderBy;
    }

    @Override
    public int compare(String left, String right) {
        FieldSet leftFields = tokenizer.tokenize(left);
        FieldSet rightFields = tokenizer.tokenize(right);

        List<String> leftValues = new ArrayList<>();
        List<String> rightValues = new ArrayList<>();

        orderBy.forEach(idx -> {
            leftValues.add(leftFields.readString(idx));
            rightValues.add(rightFields.readString(idx));
        });

        return Arrays.compare(
                leftValues.toArray(new String[] {}),
                rightValues.toArray(new String[] {}),
                Comparator.naturalOrder()
        );
    }
}
