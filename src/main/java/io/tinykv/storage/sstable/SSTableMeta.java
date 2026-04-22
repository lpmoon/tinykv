package io.tinykv.storage.sstable;

/**
 * Metadata about an SSTable file on disk.
 */
public class SSTableMeta {

    private final int level;
    private final String fileName;
    private final byte[] smallestKey;
    private final byte[] largestKey;
    private final long fileSize;
    private final long createdAt;

    public SSTableMeta(int level, String fileName, byte[] smallestKey, byte[] largestKey, long fileSize) {
        this.level = level;
        this.fileName = fileName;
        this.smallestKey = smallestKey;
        this.largestKey = largestKey;
        this.fileSize = fileSize;
        this.createdAt = System.currentTimeMillis();
    }

    public int getLevel() { return level; }
    public String getFileName() { return fileName; }
    public byte[] getSmallestKey() { return smallestKey; }
    public byte[] getLargestKey() { return largestKey; }
    public long getFileSize() { return fileSize; }
    public long getCreatedAt() { return createdAt; }

    /**
     * Check if this SSTable may contain keys in [start, end) range.
     */
    public boolean overlapsRange(byte[] start, byte[] end) {
        int cmp1 = (start != null) ? SSTableBuilder.MemTableComparator.compare(largestKey, start) : 1;
        int cmp2 = (end != null) ? SSTableBuilder.MemTableComparator.compare(smallestKey, end) : -1;
        return cmp1 >= 0 && cmp2 < 0;
    }
}
