package io.github.xlong2.raftticketing.core;

public final class TicketStateMachine {
    private final int initialTickets;
    private int remainingTickets;

    public TicketStateMachine(int initialTickets) {
        if (initialTickets < 0) {
            throw new IllegalArgumentException("initialTickets cannot be negative");
        }
        this.initialTickets = initialTickets;
        this.remainingTickets = initialTickets;
    }

    public int initialTickets() {
        return initialTickets;
    }

    public int remainingTickets() {
        return remainingTickets;
    }

    public boolean canPurchase(int tickets) {
        return tickets > 0 && tickets <= remainingTickets;
    }

    public void apply(LogEntry entry) {
        if (!canPurchase(entry.tickets())) {
            throw new IllegalStateException(
                    "committed entry would make ticket inventory negative: " + entry);
        }
        remainingTickets -= entry.tickets();
    }
}
