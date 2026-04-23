package io.tinykv.replication.common;

/**
 * Result of a replication operation.
 */
public record ReplicateResult(long index, boolean success) {}
