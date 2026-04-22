package io.tinykv.storage;

import java.io.Closeable;
import java.util.Optional;

/**
 * Iterator interface for range scan over key-value data.
 */
public interface KVIterator extends Closeable {

    boolean hasNext();

    void next();

    byte[] key();

    byte[] value();

    /**
     * Seek to the first key >= target.
     */
    void seek(byte[] target);

    /**
     * Seek to the first key.
     */
    void seekToFirst();
}
