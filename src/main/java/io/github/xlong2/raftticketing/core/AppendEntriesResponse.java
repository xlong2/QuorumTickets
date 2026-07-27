package io.github.xlong2.raftticketing.core;

import java.util.Objects;

public record AppendEntriesResponse(String followerId, int term, boolean success, int matchIndex) {
    public AppendEntriesResponse {
        Objects.requireNonNull(followerId, "followerId");
        if (term < 0 || matchIndex < 0) {
            throw new IllegalArgumentException("term and matchIndex cannot be negative");
        }
    }
}
