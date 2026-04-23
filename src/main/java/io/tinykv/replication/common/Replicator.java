package io.tinykv.replication.common;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for data replication strategies.
 *
 * Sync: delegates to Raft, waits for majority before returning.
 * Async: applies locally immediately, replicates in background (Redis-style).
 */
public interface Replicator {

    CompletableFuture<ReplicateResult> put(byte[] key, byte[] value);

    CompletableFuture<ReplicateResult> delete(byte[] key);

    CompletableFuture<ReplicateResult> batchPut(List<WriteOp> ops);

    Optional<byte[]> get(byte[] key);

    long getCommitIndex();

    void start();

    void stop();
}
