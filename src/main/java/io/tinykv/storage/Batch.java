package io.tinykv.storage;

import io.tinykv.common.Record.RecordType;

import java.util.ArrayList;
import java.util.List;

/**
 * A batch of write operations to be applied atomically.
 */
public class Batch {

    private final List<BatchEntry> entries = new ArrayList<>();

    public Batch put(byte[] key, byte[] value) {
        entries.add(new BatchEntry(key, value, RecordType.PUT));
        return this;
    }

    public Batch delete(byte[] key) {
        entries.add(new BatchEntry(key, null, RecordType.DELETE));
        return this;
    }

    public List<BatchEntry> getEntries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }

    public record BatchEntry(byte[] key, byte[] value, RecordType type) {}
}
