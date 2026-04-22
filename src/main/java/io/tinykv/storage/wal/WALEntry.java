package io.tinykv.storage.wal;

import io.tinykv.common.Record.RecordType;

/**
 * A single entry in the Write-Ahead Log.
 * Format on disk:
 *   [4 bytes: length] [4 bytes: CRC32] [1 byte: type] [4 bytes: keyLen] [key] [4 bytes: valueLen] [value]
 */
public record WALEntry(RecordType type, byte[] key, byte[] value) {

    /**
     * Compute the serialized size of this entry.
     */
    public int serializedSize() {
        int size = 4 + 4 + 1 + 4 + key.length + 4;
        size += (value != null) ? value.length : 0;
        return size;
    }
}
