package dev.rocksqueue.testing;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class MutableClock extends Clock {
    private Instant instant;
    private final ZoneId zone;

    public MutableClock(Instant initial, ZoneId zone) {
        this.instant = initial;
        this.zone = zone;
    }

    public static MutableClock startingAtMillis(long millis) {
        return new MutableClock(Instant.ofEpochMilli(millis), ZoneId.of("UTC"));
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(instant, zone);
    }

    @Override
    public Instant instant() {
        return instant;
    }

    public void advanceMillis(long delta) {
        if (delta == 0) return;
        this.instant = this.instant.plusMillis(delta);
    }

    public void setMillis(long millis) {
        this.instant = Instant.ofEpochMilli(millis);
    }
}
