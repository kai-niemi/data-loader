package io.roach.volt.util.wgs;

public class Position {
    public static Position of(double latitude, double longitude) {
        return new Position(Latitude.fromDecimal(latitude), Longitude.fromDecimal(longitude));
    }

    public static Position of(Latitude latitude, Longitude longitude) {
        return new Position(latitude, longitude);
    }

    private static final double EARTH_MEAN_RADIUS_METERS = 6371;

    /**
     * Calculates the haversine (great-circle) distance in meters between two points on a
     * spherical object, such as the earth (actually earth is an ellipsoid, but whatever).
     * <p>
     * <a href="https://en.wikipedia.org/wiki/Haversine_formula">Haversine Formula</a>
     *
     * @param from from position
     * @param to   to position
     * @return the distance in meters
     */
    public static double haversineDistance(Position from, Position to) {
        double r1 = Math.toRadians(from.getLatitude().getDegrees());
        double r2 = Math.toRadians(to.getLatitude().getDegrees());

        double latDelta = Math.toRadians(to.getLatitude().getDegrees() - from.getLatitude().getDegrees());
        double lonDelta = Math.toRadians(to.getLongitude().getDegrees() - from.getLongitude().getDegrees());

        double a = Math.sin(latDelta / 2) * Math.sin(latDelta / 2)
                + Math.cos(r1) * Math.cos(r2) * Math.sin(lonDelta / 2) * Math.sin(lonDelta / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_MEAN_RADIUS_METERS * c;
    }

    public static double bearing(Position from, Position to) {
        double r1 = Math.toRadians(from.getLatitude().getDegrees());
        double r2 = Math.toRadians(to.getLatitude().getDegrees());

        double lonDelta = Math.toRadians(to.getLongitude().getDegrees() - from.getLongitude().getDegrees());

        double x = Math.cos(r1) * Math.sin(r2) - Math.sin(r1) * Math.cos(r2) * Math.cos(lonDelta);
        double y = Math.sin(lonDelta) * Math.cos(r2);
        double d = Math.atan2(y, x);

        double degrees = Math.toDegrees(d);

        return (((2 * 180 * degrees / 360) % 360) + 360) % 360;
    }

    private final Latitude latitude;

    private final Longitude longitude;

    public Position(Latitude latitude, Longitude longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Latitude getLatitude() {
        return latitude;
    }

    public Longitude getLongitude() {
        return longitude;
    }

    public String toDMS() {
        return latitude.toDMS() + " " + longitude.toDMS();
    }
}
