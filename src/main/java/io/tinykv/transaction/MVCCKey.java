package io.tinykv.transaction;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * MVCC key encoding.
 *
 * User key is combined with a commit timestamp to form a versioned key:
 *   MVCCKey = [userKey | commitTs (8 bytes, big-endian)]
 *
 * Keys are sorted lexicographically, so:
 * - Same user key with different timestamps are sorted in descending timestamp order
 *   (we encode timestamp as ~ts to achieve reverse sort)
 * - This allows efficient scan for the latest version of a key
 */
public class MVCCKey implements Comparable<MVCCKey> {

    private static final int TIMESTAMP_SIZE = 8;

    private final byte[] userKey;
    private final long commitTs;

    public MVCCKey(byte[] userKey, long commitTs) {
        this.userKey = userKey;
        this.commitTs = commitTs;
    }

    public byte[] getUserKey() {
        return userKey;
    }

    public long getCommitTs() {
        return commitTs;
    }

    /**
     * Encode to storage key format: [userKey | ~commitTs]
     * Using ~commitTs (bitwise NOT) so that newer timestamps sort first.
     */
    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(userKey.length + TIMESTAMP_SIZE);
        buf.put(userKey);
        buf.putLong(~commitTs); // Invert for descending sort
        return buf.array();
    }

    /**
     * Decode a storage key back to MVCCKey.
     */
    public static MVCCKey decode(byte[] encoded) {
        if (encoded.length < TIMESTAMP_SIZE) {
            throw new IllegalArgumentException("Encoded key too short");
        }
        int userKeyLen = encoded.length - TIMESTAMP_SIZE;
        byte[] userKey = new byte[userKeyLen];
        System.arraycopy(encoded, 0, userKey, 0, userKeyLen);

        ByteBuffer buf = ByteBuffer.wrap(encoded, userKeyLen, TIMESTAMP_SIZE);
        long commitTs = ~buf.getLong(); // Invert back

        return new MVCCKey(userKey, commitTs);
    }

    /**
     * Create a start key for scanning a user key at a given timestamp.
     * The scan should find the first MVCCKey with userKey and commitTs <= startTs.
     */
    public static byte[] scanStartKey(byte[] userKey, long startTs) {
        return new MVCCKey(userKey, startTs).encode();
    }

    /**
     * Create an end key for scanning a user key.
     * This is just the userKey with timestamp 0 (exclusive bound).
     */
    public static byte[] scanEndKey(byte[] userKey) {
        // The key just after all versions of this userKey
        // Append a 0xFF byte to make it sort after all timestamp variants
        byte[] endKey = new byte[userKey.length + 1];
        System.arraycopy(userKey, 0, endKey, 0, userKey.length);
        endKey[userKey.length] = (byte) 0xFF;
        return endKey;
    }

    @Override
    public int compareTo(MVCCKey other) {
        int keyCmp = compareBytes(this.userKey, other.userKey);
        if (keyCmp != 0) return keyCmp;
        // Newer timestamps first (descending)
        return Long.compare(other.commitTs, this.commitTs);
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MVCCKey other)) return false;
        return commitTs == other.commitTs && Arrays.equals(userKey, other.userKey);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(userKey) + Long.hashCode(commitTs);
    }
}
