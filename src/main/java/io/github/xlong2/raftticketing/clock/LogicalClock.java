package io.github.xlong2.raftticketing.clock;

/** A small, thread-safe Lamport logical clock retained from the project's coursework origins. */
public final class LogicalClock {
    private long time;

    public LogicalClock() {
        this(0);
    }

    public LogicalClock(long initialTime) {
        if (initialTime < 0) {
            throw new IllegalArgumentException("initialTime cannot be negative");
        }
        this.time = initialTime;
    }

    public synchronized long tick() {
        time += 1;
        return time;
    }

    public synchronized long receive(long messageTimestamp) {
        if (messageTimestamp < 0) {
            throw new IllegalArgumentException("messageTimestamp cannot be negative");
        }
        time = Math.max(time, messageTimestamp) + 1;
        return time;
    }

    public synchronized long currentTime() {
        return time;
    }
}
