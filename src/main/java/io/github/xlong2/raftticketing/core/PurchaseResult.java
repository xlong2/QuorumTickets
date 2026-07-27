package io.github.xlong2.raftticketing.core;

public record PurchaseResult(
        PurchaseStatus status,
        int requestedTickets,
        int remainingTickets,
        String leaderId,
        String message) {

    public boolean committed() {
        return status == PurchaseStatus.COMMITTED;
    }
}
