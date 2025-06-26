package io.cockroachdb.dl.util.wgs;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper for a WGS-84 geodetic longitude with a ISO-6709 string representation.
 */
public class Longitude implements Comparator<Longitude>, Coordinate {
    private static final Pattern LONGITUDE
            = Pattern.compile("^([0-9]{1,2}|1[0-7][0-9]|180)°(\\s[0-5]?[0-9])′?(\\s[0-6]?[0-9](,[0-9]+)?)″([EW])?");

    private static final RoundingMode RM = RoundingMode.HALF_DOWN;

    public static Longitude fromDecimal(double coordinate) {
        return new Longitude(
                new BigDecimal(coordinate).setScale(5, RM).doubleValue());
    }

    public static Longitude fromDMS(String coordinate) {
        Matcher matcher = LONGITUDE.matcher(coordinate);
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
        if ("E".equalsIgnoreCase(sign)) {
            return new Longitude(bd.doubleValue());
        }
        return new Longitude(-bd.doubleValue());
    }

    private final double degrees;

    public Longitude(double degrees) {
        if (degrees > 180 || degrees < -180) {
            throw new IllegalArgumentException("Longitude must be 180.0 > N < -180.0");
        }
        this.degrees = degrees;
    }

    @Override
    public double getDegrees() {
        return degrees;
    }

    @Override
    public int compare(Longitude o1, Longitude o2) {
        return Double.compare(o1.degrees, o2.degrees);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Longitude longitude = (Longitude) o;
        return Double.compare(degrees, longitude.degrees) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(degrees);
    }

    @Override
    public String toDMS() {
        BigDecimal d = new BigDecimal(degrees);
        double fractions = d.subtract(new BigDecimal(d.intValue())).doubleValue();
        BigDecimal m = new BigDecimal(fractions).multiply(new BigDecimal(60));
        double s = m.subtract(new BigDecimal(m.intValue()))
                .multiply(new BigDecimal(60))
                .doubleValue();
        return "%d° %d′ %.3f″%s".formatted(
                Math.abs(d.intValue()),
                Math.abs(m.intValue()),
                Math.abs(s),
                degrees >= 0 ? "E" : "W");
    }

    @Override
    public String toString() {
        return "Longitude{" +
                "coordinate=" + degrees +
                ", dms=" + toDMS() +
                '}';
    }
}
