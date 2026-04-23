package io.tinykv.storage;

import io.tinykv.replication.common.CommandCodec;
import io.tinykv.replication.common.WriteOp;
import io.tinykv.transaction.MVCCKey;
import io.tinykv.transaction.TimestampOracle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * MVCC-transparent storage layer wrapping LSMTree.
 *
 * All reads and writes automatically go through MVCC semantics:
 * - Every put() gets an automatic commitTs (from TimestampOracle)
 * - Every get() reads at the latest committed version (auto-readTs)
 * - Scans are MVCC-aware and return only versions visible at the latest commitTs
 *
 * Internal storage: MVCCKey(userKey, commitTs).encode() = [userKey | ~commitTs]
 * This encoding makes newer timestamps sort first lexicographically.
 */
public class MVCCStorage {

    private static final Logger LOG = LoggerFactory.getLogger(MVCCStorage.class);

    private final LSMTree engine;
    private final TimestampOracle tsOracle;

    // Tracks the latest committed ts for auto-readTs
    private volatile long lastCommitTs = 0;

    public MVCCStorage(LSMTree engine, TimestampOracle tsOracle) {
        this.engine = engine;
        this.tsOracle = tsOracle;
    }

    /**
     * Transparent put: auto-assigns commitTs and stores as MVCC version.
     */
    public void put(byte[] userKey, byte[] value) {
        long commitTs = tsOracle.next();
        byte[] internalKey = new MVCCKey(userKey, commitTs).encode();
        engine.put(internalKey, value);
        lastCommitTs = Math.max(lastCommitTs, commitTs);
        LOG.debug("MVCCStorage.put: userKey={}, commitTs={}", hex(userKey), commitTs);
    }

    /**
     * Transparent delete: stores a tombstone as a MVCC version.
     */
    public void delete(byte[] userKey) {
        long commitTs = tsOracle.next();
        byte[] internalKey = new MVCCKey(userKey, commitTs).encode();
        engine.delete(internalKey);
        lastCommitTs = Math.max(lastCommitTs, commitTs);
        LOG.debug("MVCCStorage.delete: userKey={}, commitTs={}", hex(userKey), commitTs);
    }

    /**
     * Transparent get: reads at the latest committed version (auto-readTs).
     */
    public Optional<byte[]> get(byte[] userKey) throws IOException {
        return get(userKey, lastCommitTs);
    }

    /**
     * MVCC-aware get: returns the latest version visible at the given readTs.
     * Scans all MVCC versions of userKey, returns the one with largest commitTs <= readTs.
     * If that version is a tombstone (deleted), returns empty.
     */
    public Optional<byte[]> get(byte[] userKey, long readTs) throws IOException {
        byte[] startKey = MVCCKey.scanStartKey(userKey, readTs);
        byte[] endKey = MVCCKey.scanEndKey(userKey);

        try (KVIterator it = engine.scan(startKey, endKey)) {
            while (it.hasNext()) {
                it.next();
                byte[] internalKey = it.key();
                MVCCKey mvccKey = MVCCKey.decode(internalKey);

                // Safety check: we've moved past this userKey
                if (!bytesEqual(mvccKey.getUserKey(), userKey)) {
                    break;
                }

                // commitTs <= readTs means this version is visible
                if (mvccKey.getCommitTs() <= readTs) {
                    byte[] value = it.value();
                    LOG.debug("MVCCStorage.get: userKey={}, readTs={}, found commitTs={}, value={}",
                            hex(userKey), readTs, mvccKey.getCommitTs(), value != null ? "present" : "tombstone");
                    return Optional.ofNullable(value);
                }
            }
        }

        LOG.debug("MVCCStorage.get: userKey={}, readTs={}, not found", hex(userKey), readTs);
        return Optional.empty();
    }

    /**
     * Transparent scan: returns MVCC-aware iterator over [startKey, endKey).
     */
    public KVIterator scan(byte[] startKey, byte[] endKey) {
        return new MVCCIterator(engine.scan(startKey, endKey), lastCommitTs);
    }

    /**
     * MVCC-aware scan: iterator only returns versions visible at the given readTs.
     */
    public KVIterator scan(byte[] startKey, byte[] endKey, long readTs) {
        return new MVCCIterator(engine.scan(startKey, endKey), readTs);
    }

    /**
     * Write a batch of already-MVCC-encoded internal keys.
     * Used by Txn.commit() which pre-encodes keys as MVCCKey(userKey, commitTs).
     * This bypasses MVCC encoding — the keys are already encoded.
     */
    public void writeTxnBatch(List<WriteOp> ops) {
        Batch batch = new Batch();
        for (WriteOp op : ops) {
            if (op.type() == CommandCodec.OpType.PUT) {
                batch.put(op.key(), op.value());
            } else {
                batch.delete(op.key());
            }
        }
        engine.write(batch);
    }

    /**
     * Check for write-write conflict on a specific userKey.
     * Returns true if there exists a committed version with commitTs > startTs.
     * Used by Txn.hasConflict() for snapshot isolation.
     *
     * This scans ALL versions (not filtered by readTs) by using scanEnd as the bound
     * and checking commitTs directly.
     */
    public boolean hasConflict(byte[] userKey, long startTs) throws IOException {
        byte[] scanStart = MVCCKey.scanStartKey(userKey, Long.MAX_VALUE);
        byte[] scanEnd = MVCCKey.scanEndKey(userKey);

        try (KVIterator it = engine.scan(scanStart, scanEnd)) {
            while (it.hasNext()) {
                it.next();
                MVCCKey mvccKey = MVCCKey.decode(it.key());
                if (!bytesEqual(mvccKey.getUserKey(), userKey)) {
                    break;
                }
                // If a version was committed AFTER our startTs, it's a conflict
                if (mvccKey.getCommitTs() > startTs) {
                    return true;
                }
            }
        }
        return false;
    }

    public LSMTree getEngine() {
        return engine;
    }

    public long getLastCommitTs() {
        return lastCommitTs;
    }

    private static boolean bytesEqual(byte[] a, byte[] b) {
        if (a == null || b == null) return false;
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    private static String hex(byte[] key) {
        if (key == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (byte b : key) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
