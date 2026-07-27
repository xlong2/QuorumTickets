package io.github.xlong2.raftticketing.core;

public record LogEntry(int index, int term, int tickets) {
    public LogEntry {
        if (index < 1) {
            throw new IllegalArgumentException("index must be positive");
        }
        if (term < 1) {
            throw new IllegalArgumentException("term must be positive");
        }
        if (tickets < 1) {
            throw new IllegalArgumentException("tickets must be positive");
        }
    }
}
