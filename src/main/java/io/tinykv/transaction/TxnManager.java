package io.tinykv.transaction;

import io.tinykv.replication.sync.SyncReplicator;
import io.tinykv.storage.MVCCStorage;

/**
 * Manages transaction lifecycle.
 * Provides begin/commit/abort operations and coordinates with MVCC.
 */
public class TxnManager {

    private final MVCCStorage mvccStorage;
    private final SyncReplicator replicator;
    private final TimestampOracle tsOracle;

    public TxnManager(MVCCStorage mvccStorage, SyncReplicator replicator, TimestampOracle tsOracle) {
        this.mvccStorage = mvccStorage;
        this.replicator = replicator;
        this.tsOracle = tsOracle;
    }

    /**
     * Begin a new transaction.
     */
    public Txn begin() {
        long startTs = tsOracle.next();
        return new Txn(startTs, mvccStorage, replicator, tsOracle);
    }

    /**
     * Execute a callback within a transaction. Auto-commits on success, auto-aborts on failure.
     */
    public void execute(TxnCallback callback) throws Exception {
        Txn txn = begin();
        try {
            callback.run(txn);
            txn.commit();
        } catch (Exception e) {
            txn.abort();
            throw e;
        }
    }

    /**
     * Callback interface for transactional execution.
     */
    @FunctionalInterface
    public interface TxnCallback {
        void run(Txn txn) throws Exception;
    }
}
