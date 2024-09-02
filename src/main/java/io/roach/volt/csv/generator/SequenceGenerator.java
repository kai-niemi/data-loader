package io.roach.volt.csv.generator;

import io.roach.volt.csv.model.Gen;
import io.roach.volt.util.Multiplier;
import org.springframework.util.StringUtils;

import java.util.concurrent.atomic.AtomicLong;

public class SequenceGenerator implements ValueGenerator<Long> {
    private final long startNumber;

    private final long stopNumber;

    private final AtomicLong nextNumber = new AtomicLong();

    private final int increment;

    public SequenceGenerator(Gen gen) {
        this.increment = Math.max(1, gen.getStep());

        if (StringUtils.hasLength(gen.getFrom())) {
            this.startNumber = Multiplier.parseLong(gen.getFrom());
        } else {
            this.startNumber = 1;
        }

        if (StringUtils.hasLength(gen.getTo())) {
            this.stopNumber = Multiplier.parseLong(gen.getTo());
        } else {
            this.stopNumber = Long.MAX_VALUE;
        }

        this.nextNumber.set(startNumber);
    }

    @Override
    public Long nextValue() {
        long next = nextNumber.get();
        if (nextNumber.addAndGet(increment) > stopNumber) {
            nextNumber.set(startNumber);
        }
        return next;
    }
}
