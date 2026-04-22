package io.tinykv.raft;

/**
 * A single Raft log entry.
 */
public record LogEntry(long term, long index, byte[] data) {

    public LogEntry {
        if (term < 0) throw new IllegalArgumentException("term must be >= 0");
        if (index < 0) throw new IllegalArgumentException("index must be >= 0");
    }
}
