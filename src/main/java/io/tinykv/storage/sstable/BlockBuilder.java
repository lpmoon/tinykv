package io.tinykv.storage.sstable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds a single data block with prefix-compressed key-value entries.
 *
 * Restart points are inserted every N entries so that binary search
 * can be used within a block to find keys efficiently.
 */
public class BlockBuilder {

    private static final int RESTART_INTERVAL = 16;

    private final List<byte[]> entries = new ArrayList<>();
    private final List<Integer> restartOffsets = new ArrayList<>();
    private int estimatedSize;
    private byte[] lastKey;

    public BlockBuilder() {
        this.estimatedSize = 0;
        this.restartOffsets.add(0);
    }

    public void add(byte[] key, byte[] value) {
        int sharedLen = 0;
        if (lastKey != null && entries.size() % RESTART_INTERVAL != 0) {
            sharedLen = sharedPrefixLen(lastKey, key);
        }

        int unsharedLen = key.length - sharedLen;
        int valueLen = (value != null) ? value.length : 0;

        // varint sizes (simplified: use fixed 4-byte ints for now)
        int entrySize = 4 + 4 + 4 + unsharedLen + valueLen;
        entries.add(encodeEntry(sharedLen, unsharedLen, valueLen, key, value, sharedLen));
        estimatedSize += entrySize;

        if (entries.size() % RESTART_INTERVAL == 0) {
            restartOffsets.add(estimatedSize);
        }

        lastKey = key.clone();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int estimatedSize() {
        return estimatedSize + restartOffsets.size() * 4 + 4; // restarts array + count
    }

    /**
     * Finish the block and return the serialized bytes.
     */
    public byte[] finish() {
        int restartArraySize = restartOffsets.size() * 4 + 4;
        ByteBuffer buf = ByteBuffer.allocate(estimatedSize + restartArraySize);

        for (byte[] entry : entries) {
            buf.put(entry);
        }

        for (int offset : restartOffsets) {
            buf.putInt(offset);
        }
        buf.putInt(restartOffsets.size());

        return buf.array();
    }

    public void reset() {
        entries.clear();
        restartOffsets.clear();
        restartOffsets.add(0);
        estimatedSize = 0;
        lastKey = null;
    }

    private byte[] encodeEntry(int sharedLen, int unsharedLen, int valueLen,
                               byte[] key, byte[] value, int keyOffset) {
        int size = 4 + 4 + 4 + unsharedLen + valueLen;
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(sharedLen);
        buf.putInt(unsharedLen);
        buf.putInt(valueLen);
        buf.put(key, keyOffset, unsharedLen);
        if (value != null && valueLen > 0) {
            buf.put(value);
        }
        return buf.array();
    }

    private int sharedPrefixLen(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        int shared = 0;
        while (shared < minLen && a[shared] == b[shared]) {
            shared++;
        }
        return shared;
    }
}
