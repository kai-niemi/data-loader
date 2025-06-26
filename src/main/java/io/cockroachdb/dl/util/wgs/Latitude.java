package io.cockroachdb.dl.util.wgs;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper for a WGS-84 geodetic latitude with a ISO-6709 string representation.
 */
public class Latitude implements Comparator<Latitude>, Coordinate {
    private static final Pattern LATITUDE
            = Pattern.compile("^([0-8]?[0-9]|90)°(\\s[0-5]?[0-9])′?(\\s[0-6]?[0-9](,[0-9]+)?)″([NS])?");

    private static final RoundingMode RM = RoundingMode.HALF_DOWN;

    public static Latitude fromDecimal(double coordinate) {
        return new Latitude(
                new BigDecimal(coordinate).setScale(5, RM).doubleValue());
    }

    public static Latitude fromDMS(String coordinate) {
        Matcher matcher = LATITUDE.matcher(coordinate);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(coordinate);
        }
        int d = Integer.parseInt(matcher.group(1));
        int m = Integer.parseInt(matcher.group(2).trim());
        double s = Double.parseDouble(matcher.group(3).replace(",", ".").trim());
        BigDecimal bd = new BigDecimal(d)
                .add(new BigDecimal(m)
                        .divide(new BigDecimal("60"), 5, RM))
                .add(new BigDecimal(s)
                        .divide(new BigDecimal("3600"), 5, RM));
        String sign = matcher.group(5);
        if ("N".equalsIgnoreCase(sign)) {
            return new Latitude(bd.doubleValue());
        }
        return new Latitude(-bd.doubleValue());
    }

    private final double coordinate;

    public Latitude(double coordinate) {
        if (coordinate > 90 || coordinate < -90) {
            throw new IllegalArgumentException("Latitude must be 90.0 > N < -90.0");
        }
        this.coordinate = coordinate;
    }

    @Override
    public double getDegrees() {
        return coordinate;
    }

    @Override
    public int compare(Latitude o1, Latitude o2) {
        return Double.compare(o1.coordinate, o2.coordinate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Latitude latitude = (Latitude) o;
        return Double.compare(coordinate, latitude.coordinate) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(coordinate);
    }

    @Override
    public String toDMS() {
        BigDecimal d = new BigDecimal(coordinate);
        BigDecimal fractions = d.subtract(new BigDecimal(d.intValue()));
        BigDecimal m = fractions.multiply(new BigDecimal(60));
        double s = m.subtract(new BigDecimal(m.intValue()))
                .multiply(new BigDecimal(60))
                .doubleValue();
        return "%d° %d′ %.3f″%s".formatted(
                Math.abs(d.intValue()),
                Math.abs(m.intValue()),
                Math.abs(s),
                coordinate >= 0 ? "N" : "S");
    }

    @Override
    public String toString() {
        return "Latitude{" +
                "coordinate=" + coordinate +
                ", dms=" + toDMS() +
                '}';
    }
}
