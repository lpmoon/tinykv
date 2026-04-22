package io.tinykv.storage.sstable;

/**
 * Represents a data block within an SSTable.
 * A block is the basic unit of I/O and caching.
 *
 * On-disk format:
 *   [entries...] [restarts: int[]] [numRestarts: int]
 *
 * Each entry uses prefix compression with periodic restart points.
 * Entry format:
 *   [sharedLen: varint] [unsharedLen: varint] [valueLen: varint] [unsharedKey] [value]
 */
public class Block {

    private final byte[] data;
    private final int[] restartOffsets;
    private final int numRestarts;

    public Block(byte[] data, int[] restartOffsets) {
        this.data = data;
        this.restartOffsets = restartOffsets;
        this.numRestarts = restartOffsets.length;
    }

    public byte[] getData() {
        return data;
    }

    public int[] getRestartOffsets() {
        return restartOffsets;
    }

    public int getNumRestarts() {
        return numRestarts;
    }
}
