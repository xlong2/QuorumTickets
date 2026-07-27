package io.github.xlong2.raftticketing.core;

public record NodeStatus(
        String nodeId,
        Role role,
        int term,
        String leaderId,
        int commitIndex,
        int lastApplied,
        int lastLogIndex,
        int remainingTickets) {}
