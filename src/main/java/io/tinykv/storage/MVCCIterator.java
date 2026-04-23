package io.tinykv.storage;

import io.tinykv.transaction.MVCCKey;

import java.util.NoSuchElementException;

/**
 * MVCC-aware iterator over a raw KVIterator.
 *
 * Since MVCCKey encodes ~commitTs (descending sort), the raw iterator returns
 * newest versions first. This iterator groups entries by userKey and returns
 * only the latest version where commitTs <= readTs.
 *
 * Entries with commitTs > readTs are skipped (they're from the future).
 * Tombstones (null value) are treated as "deleted at that commitTs".
 */
public class MVCCIterator implements KVIterator {

    private final KVIterator inner;
    private final long readTs;

    private byte[] currentKey;
    private byte[] currentValue;
    private boolean hasCurrent;
    private boolean initialized = false;

    public MVCCIterator(KVIterator inner, long readTs) {
        this.inner = inner;
        this.readTs = readTs;
    }

    @Override
    public boolean hasNext() {
        if (!initialized) {
            advance();
            initialized = true;
        }
        return hasCurrent;
    }

    @Override
    public void next() {
        if (!hasCurrent) {
            throw new NoSuchElementException();
        }
        hasCurrent = false;
        // Advance to next userKey for subsequent call
        advance();
    }

    @Override
    public byte[] key() {
        if (!initialized || !hasCurrent) {
            throw new NoSuchElementException();
        }
        return currentKey;
    }

    @Override
    public byte[] value() {
        if (!initialized || !hasCurrent) {
            throw new NoSuchElementException();
        }
        return currentValue;
    }

    @Override
    public void seek(byte[] target) {
        inner.seek(target);
        hasCurrent = false;
        initialized = false;
        currentKey = null;
        currentValue = null;
    }

    @Override
    public void seekToFirst() {
        inner.seekToFirst();
        hasCurrent = false;
        initialized = false;
        currentKey = null;
        currentValue = null;
    }

    @Override
    public void close() {
        try {
            inner.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Advance to the next valid (userKey, version) pair visible at readTs.
     *
     * Strategy: Since raw iterator returns newest version first,
     * we skip all versions of a userKey until we find one with commitTs <= readTs.
     * Once we find it, that's the answer for this userKey.
     */
    private void advance() {
        hasCurrent = false;

        while (inner.hasNext()) {
            inner.next();
            byte[] rawKey = inner.key();
            MVCCKey mvccKey = MVCCKey.decode(rawKey);

            // If this version is visible (committed before readTs), it's our answer for this userKey
            if (mvccKey.getCommitTs() <= readTs) {
                currentKey = mvccKey.getUserKey();
                currentValue = inner.value(); // null = tombstone
                hasCurrent = true;
                return;
            }
            // commitTs > readTs: skip this version
            // Since iterator is sorted by ~commitTs (newest first),
            // ALL remaining versions for this userKey will also have commitTs > readTs,
            // so we skip to the next userKey entirely
        }
    }
}
