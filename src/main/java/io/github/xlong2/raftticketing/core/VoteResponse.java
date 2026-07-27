package io.github.xlong2.raftticketing.core;

import java.util.Objects;

public record VoteResponse(String voterId, int term, boolean voteGranted) {
    public VoteResponse {
        Objects.requireNonNull(voterId, "voterId");
        if (term < 0) {
            throw new IllegalArgumentException("term cannot be negative");
        }
    }
}
