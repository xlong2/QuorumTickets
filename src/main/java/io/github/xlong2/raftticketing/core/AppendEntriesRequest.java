package io.github.xlong2.raftticketing.core;

import java.util.List;
import java.util.Objects;

public record AppendEntriesRequest(
        int term,
        String leaderId,
        int previousLogIndex,
        int previousLogTerm,
        List<LogEntry> entries,
        int leaderCommit) {

    public AppendEntriesRequest {
        if (term < 1) {
            throw new IllegalArgumentException("term must be positive");
        }
        Objects.requireNonNull(leaderId, "leaderId");
        if (previousLogIndex < 0 || previousLogTerm < 0 || leaderCommit < 0) {
            throw new IllegalArgumentException("log coordinates cannot be negative");
        }
        entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
    }
}
