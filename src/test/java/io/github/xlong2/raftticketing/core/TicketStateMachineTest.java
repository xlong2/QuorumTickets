package io.github.xlong2.raftticketing.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TicketStateMachineTest {
    @Test
    void appliesCommittedPurchasesWithoutOverselling() {
        TicketStateMachine stateMachine = new TicketStateMachine(10);

        assertTrue(stateMachine.canPurchase(6));
        stateMachine.apply(new LogEntry(1, 1, 6));

        assertEquals(4, stateMachine.remainingTickets());
        assertFalse(stateMachine.canPurchase(5));
        assertThrows(
                IllegalStateException.class,
                () -> stateMachine.apply(new LogEntry(2, 1, 5)));
        assertEquals(4, stateMachine.remainingTickets());
    }

    @Test
    void validatesInitialInventory() {
        assertThrows(IllegalArgumentException.class, () -> new TicketStateMachine(-1));
    }
}
