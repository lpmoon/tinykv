package io.tinykv.common;

/**
 * Key-Value record with MVCC timestamp.
 */
public record Record(byte[] key, byte[] value, long timestamp, RecordType type) {

    public enum RecordType {
        PUT, DELETE
    }

    public boolean isDelete() {
        return type == RecordType.DELETE;
    }
}
