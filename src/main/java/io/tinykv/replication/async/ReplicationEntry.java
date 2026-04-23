package io.tinykv.replication.async;

/**
 * A single entry in the async replication log.
 */
public record ReplicationEntry(long index, byte[] data) {}
