package io.tinykv.replication.common;

/**
 * A single write operation (PUT or DELETE) used in batch replication.
 */
public record WriteOp(CommandCodec.OpType type, byte[] key, byte[] value) {}
