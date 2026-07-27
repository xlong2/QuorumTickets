package io.github.xlong2.raftticketing.core;

public enum PurchaseStatus {
    COMMITTED,
    REJECTED,
    NOT_LEADER,
    NO_QUORUM,
    INVALID
}
