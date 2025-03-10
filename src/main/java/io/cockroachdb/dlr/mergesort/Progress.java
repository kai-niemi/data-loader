package io.cockroachdb.dlr.mergesort;

public class Progress {
    enum Unit {
        bytes, count
    }

    private long current;

    private long total;

    private Unit unit;

    private String note;

    public long getCurrent() {
        return current;
    }

    public long getTotal() {
        return total;
    }

    public Unit getUnit() {
        return unit;
    }

    public String getNote() {
        return note;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private long current;
        private long total;
        private Unit unit;

        private String note;

        private Builder() {
        }

        public Builder withCurrent(long current) {
            this.current = current;
            return this;
        }

        public Builder withTotal(long total) {
            this.total = total;
            return this;
        }

        public Builder withUnit(Unit unit) {
            this.unit = unit;
            return this;
        }

        public Builder withNote(String note) {
            this.note = note;
            return this;
        }

        public Progress build() {
            Progress progress = new Progress();
            progress.current = this.current;
            progress.unit = this.unit;
            progress.total = this.total;
            progress.note = this.note;
            return progress;
        }
    }
}
