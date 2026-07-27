package io.github.xlong2.raftticketing.clock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LogicalClockTest {
    @Test
    void advancesForLocalAndRemoteEvents() {
        LogicalClock clock = new LogicalClock();

        assertEquals(1, clock.tick());
        assertEquals(8, clock.receive(7));
        assertEquals(9, clock.receive(4));
        assertEquals(9, clock.currentTime());
    }

    @Test
    void rejectsNegativeTime() {
        assertThrows(IllegalArgumentException.class, () -> new LogicalClock(-1));
        assertThrows(IllegalArgumentException.class, () -> new LogicalClock().receive(-1));
    }
}
