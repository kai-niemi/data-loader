package io.cockroachdb.dlr.mergesort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FastComparator implements Comparator<String> {
    private final Pattern csvPattern;

    private final List<Integer> orderBy;

    public FastComparator(String delimiter, List<Integer> orderBy) {
        Pattern p = Pattern.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?:,|$)",
                Pattern.CASE_INSENSITIVE);
        if (!",".equals(delimiter)) {
            p = Pattern.compile(p.pattern().replace(",", delimiter),
                    Pattern.CASE_INSENSITIVE);
        }
        this.csvPattern = p;
        this.orderBy = orderBy;
    }

    @Override
    public int compare(String left, String right) {
        List<String> leftFields = tokenize(left);
        List<String> rightFields = tokenize(right);

        List<String> leftValues = new ArrayList<>();
        List<String> rightValues = new ArrayList<>();

        orderBy.forEach(idx -> {
            leftValues.add(leftFields.get(idx));
            rightValues.add(rightFields.get(idx));
        });

        return Arrays.compare(
                leftValues.toArray(new String[] {}),
                rightValues.toArray(new String[] {}),
                Comparator.naturalOrder());
    }

    private List<String> tokenize(String line) {
        Matcher matcher = csvPattern.matcher(line);
        List<String> matches = new ArrayList<>();

        while (matcher.find()) {
            String match = matcher.group(1);
            if (match != null) {
                matches.add(match);
            } else {
                matches.add(matcher.group(2));
            }
        }

        return matches;
    }
}
