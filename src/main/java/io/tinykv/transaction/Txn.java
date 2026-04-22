package io.tinykv.transaction;

import io.tinykv.replication.SyncReplicator;
import io.tinykv.storage.KVIterator;
import io.tinykv.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * Transaction implementation with MVCC Snapshot Isolation.
 *
 * Lifecycle:
 * 1. begin()  → obtains start_ts
 * 2. get()    → reads the latest committed version before start_ts
 * 3. put()    → buffers writes locally
 * 4. commit() → obtains commit_ts, writes all versions atomically via Raft
 *
 * Conflict detection:
 * - If another transaction committed a write to the same key after our start_ts,
 *   we have a write-write conflict and must abort.
 */
public class Txn {

    private static final Logger LOG = LoggerFactory.getLogger(Txn.class);

    private final long startTs;
    private final StorageEngine storageEngine;
    private final SyncReplicator replicator;
    private final TimestampOracle tsOracle;

    // Local write buffer: userKey -> value
    private final TreeMap<byte[], byte[]> writeBuffer = new TreeMap<>(Txn::compareBytes);
    private final Set<byte[]> deleteSet = new TreeSet<>(Txn::compareBytes);

    private boolean committed = false;
    private boolean aborted = false;

    Txn(long startTs, StorageEngine storageEngine, SyncReplicator replicator, TimestampOracle tsOracle) {
        this.startTs = startTs;
        this.storageEngine = storageEngine;
        this.replicator = replicator;
        this.tsOracle = tsOracle;
    }

    /**
     * Read the latest committed version of a key visible at start_ts.
     */
    public Optional<byte[]> get(byte[] userKey) throws IOException {
        if (committed || aborted) throw new IllegalStateException("Transaction already finished");

        // Check local write buffer first
        if (deleteSet.contains(userKey)) {
            return Optional.empty();
        }
        byte[] buffered = writeBuffer.get(userKey);
        if (buffered != null) {
            return Optional.of(buffered);
        }

        // Read from storage engine: scan MVCC keys for this user key
        byte[] scanStart = MVCCKey.scanStartKey(userKey, startTs);
        byte[] scanEnd = MVCCKey.scanEndKey(userKey);

        try (KVIterator it = storageEngine.scan(scanStart, scanEnd)) {
            while (it.hasNext()) {
                it.next();
                byte[] rawKey = it.key();
                byte[] value = it.value();

                MVCCKey mvccKey = MVCCKey.decode(rawKey);
                if (mvccKey.getCommitTs() <= startTs) {
                    if (value == null || value.length == 0) {
                        return Optional.empty(); // Deleted
                    }
                    return Optional.of(value);
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Buffer a write (not visible to other transactions until commit).
     */
    public void put(byte[] userKey, byte[] value) {
        if (committed || aborted) throw new IllegalStateException("Transaction already finished");
        writeBuffer.put(userKey, value);
        deleteSet.remove(userKey);
    }

    /**
     * Buffer a delete.
     */
    public void delete(byte[] userKey) {
        if (committed || aborted) throw new IllegalStateException("Transaction already finished");
        deleteSet.add(userKey);
        writeBuffer.remove(userKey);
    }

    /**
     * Commit the transaction.
     * 1. Obtain commit_ts
     * 2. Check for write-write conflicts
     * 3. Write all versions atomically via Raft
     */
    public void commit() throws InterruptedException, TimeoutException, IOException {
        if (committed || aborted) throw new IllegalStateException("Transaction already finished");

        if (writeBuffer.isEmpty() && deleteSet.isEmpty()) {
            committed = true;
            return;
        }

        long commitTs = tsOracle.next();

        // Check for write-write conflicts
        if (hasConflict(commitTs)) {
            abort();
            throw new ConflictException("Write-write conflict detected");
        }

        // Build the list of MVCC writes
        List<SyncReplicator.WriteOp> ops = new ArrayList<>();

        for (Map.Entry<byte[], byte[]> entry : writeBuffer.entrySet()) {
            byte[] mvccKey = new MVCCKey(entry.getKey(), commitTs).encode();
            ops.add(new SyncReplicator.WriteOp(SyncReplicator.OpType.PUT, mvccKey, entry.getValue()));
        }

        for (byte[] userKey : deleteSet) {
            byte[] mvccKey = new MVCCKey(userKey, commitTs).encode();
            ops.add(new SyncReplicator.WriteOp(SyncReplicator.OpType.DELETE, mvccKey, null));
        }

        // Write atomically through Raft
        replicator.batchPut(ops);

        committed = true;
        LOG.debug("Transaction committed: startTs={}, commitTs={}, ops={}", startTs, commitTs, ops.size());
    }

    /**
     * Abort the transaction. All buffered writes are discarded.
     */
    public void abort() {
        aborted = true;
        writeBuffer.clear();
        deleteSet.clear();
        LOG.debug("Transaction aborted: startTs={}", startTs);
    }

    /**
     * Check for write-write conflicts: if any key we want to write has been
     * committed by another transaction after our start_ts, we have a conflict.
     */
    private boolean hasConflict(long commitTs) throws IOException {
        Set<byte[]> allKeys = new TreeSet<>(Txn::compareBytes);
        allKeys.addAll(writeBuffer.keySet());
        allKeys.addAll(deleteSet);

        for (byte[] userKey : allKeys) {
            byte[] scanStart = MVCCKey.scanStartKey(userKey, Long.MAX_VALUE);
            byte[] scanEnd = MVCCKey.scanEndKey(userKey);

            try (KVIterator it = storageEngine.scan(scanStart, scanEnd)) {
                while (it.hasNext()) {
                    it.next();
                    MVCCKey mvccKey = MVCCKey.decode(it.key());
                    // If there's a version committed after our start_ts, it's a conflict
                    if (mvccKey.getCommitTs() > startTs) {
                        LOG.debug("Write-write conflict on key: committedTs={}, startTs={}",
                                mvccKey.getCommitTs(), startTs);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public long getStartTs() { return startTs; }
    public boolean isCommitted() { return committed; }
    public boolean isAborted() { return aborted; }

    private static int compareBytes(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    /**
     * Exception thrown when a write-write conflict is detected.
     */
    public static class ConflictException extends RuntimeException {
        public ConflictException(String message) {
            super(message);
        }
    }
}
