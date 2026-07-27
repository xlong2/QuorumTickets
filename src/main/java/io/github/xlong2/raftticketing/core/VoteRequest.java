package io.github.xlong2.raftticketing.core;

import java.util.Objects;

public record VoteRequest(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
    public VoteRequest {
        if (term < 1) {
            throw new IllegalArgumentException("term must be positive");
        }
        Objects.requireNonNull(candidateId, "candidateId");
        if (lastLogIndex < 0 || lastLogTerm < 0) {
            throw new IllegalArgumentException("log coordinates cannot be negative");
        }
    }
}
