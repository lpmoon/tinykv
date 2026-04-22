package io.tinykv.storage;

import java.io.Closeable;
import java.util.Optional;

/**
 * Core storage engine interface.
 * Implementations provide a persistent KV store with range scan support.
 */
public interface StorageEngine extends Closeable {

    /**
     * Put a key-value pair.
     */
    void put(byte[] key, byte[] value);

    /**
     * Get value by key. Returns empty if not found or deleted.
     */
    Optional<byte[]> get(byte[] key);

    /**
     * Delete a key.
     */
    void delete(byte[] key);

    /**
     * Atomically write a batch of operations.
     * Either all succeed or none do.
     */
    void write(Batch batch);

    /**
     * Scan keys in [startKey, endKey) range.
     */
    KVIterator scan(byte[] startKey, byte[] endKey);

    /**
     * Recover state from WAL on startup.
     */
    void recover();

    /**
     * Flush memtable to SSTable (compaction trigger).
     */
    void flush();
}
