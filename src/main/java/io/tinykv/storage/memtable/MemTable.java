package io.tinykv.storage.memtable;

import io.tinykv.storage.KVIterator;

/**
 * In-memory sorted KV store interface.
 * Thread-safe for concurrent reads and writes.
 */
public interface MemTable {

    /**
     * Put a key-value pair.
     */
    void put(byte[] key, byte[] value);

    /**
     * Delete a key (insert tombstone).
     */
    void delete(byte[] key);

    /**
     * Get the value for a key, or null if not found or deleted.
     */
    byte[] get(byte[] key);

    /**
     * Check if a key exists (not deleted).
     */
    boolean contains(byte[] key);

    /**
     * Check if a key has a tombstone.
     */
    boolean isDeleted(byte[] key);

    /**
     * Approximate memory usage in bytes.
     */
    long approximateSize();

    /**
     * Check if memtable is empty.
     */
    boolean isEmpty();

    /**
     * Number of entries (including tombstones).
     */
    int entryCount();

    /**
     * Iterate over all entries.
     */
    KVIterator iterator();

    /**
     * Iterate over entries in [startKey, endKey) range.
     */
    KVIterator iterator(byte[] startKey, byte[] endKey);

    /**
     * Unsigned lexicographic byte array comparison.
     */
    static int compareBytes(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }
}
