package io.tinykv.replication.async;

/**
 * Result of a WAIT command (only applicable to AsyncReplicator).
 */
public record WaitResult(int replicatedCount, boolean success) {}
