package io.tinykv.storage.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Builds an SSTable file from sorted key-value pairs.
 *
 * SSTable file format:
 *   [Data Block 1] [Data Block 2] ... [Data Block N]
 *   [Meta Block (Bloom Filter)]
 *   [Index Block]  (one entry per data block: largestKey -> blockOffset, blockSize)
 *   [Footer]
 *
 * Footer format (48 bytes):
 *   [8: indexBlockOffset] [8: indexBlockSize] [8: metaBlockOffset] [8: metaBlockSize] [4: magicNumber]
 */
public class SSTableBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(SSTableBuilder.class);
    private static final int MAGIC_NUMBER = 0x54494E59; // "TINY"
    private static final int TARGET_BLOCK_SIZE = 4096;

    private final Path filePath;
    private final int blockSize;

    private BlockBuilder dataBlockBuilder;
    private BlockBuilder indexBlockBuilder;
    private final List<BlockMeta> dataBlocks = new ArrayList<>();
    private byte[] lastKey;
    private long offset;

    // Bloom filter bits
    private long[] bloomFilter;
    private int bloomBitsPerKey;
    private int numKeys;

    public SSTableBuilder(Path filePath, int blockSize, int bloomBitsPerKey) {
        this.filePath = filePath;
        this.blockSize = blockSize;
        this.bloomBitsPerKey = bloomBitsPerKey;
        this.dataBlockBuilder = new BlockBuilder();
        this.indexBlockBuilder = new BlockBuilder();
        this.offset = 0;
        this.numKeys = 0;
    }

    public void add(byte[] key, byte[] value) throws IOException {
        if (lastKey != null && MemTableComparator.compare(key, lastKey) <= 0) {
            throw new IllegalArgumentException("Keys must be added in sorted order");
        }

        if (dataBlockBuilder.estimatedSize() >= blockSize) {
            flushDataBlock();
        }

        dataBlockBuilder.add(key, value);
        lastKey = key.clone();
        numKeys++;
    }

    public long finish() throws IOException {
        if (!dataBlockBuilder.isEmpty()) {
            flushDataBlock();
        }

        // Build bloom filter
        buildBloomFilter();

        // Write bloom filter as meta block
        long metaOffset = offset;
        byte[] metaBlockData = serializeBloomFilter();
        writeFile(metaBlockData);
        long metaSize = metaBlockData.length;

        // Write index block
        long indexOffset = offset;
        byte[] indexBlockData = indexBlockBuilder.finish();
        writeFile(indexBlockData);
        long indexSize = indexBlockData.length;

        // Write footer
        ByteBuffer footer = ByteBuffer.allocate(40);
        footer.putLong(indexOffset);
        footer.putLong(indexSize);
        footer.putLong(metaOffset);
        footer.putLong(metaSize);
        footer.putInt(MAGIC_NUMBER);
        writeFile(footer.array());

        LOG.info("Built SSTable: {} ({} keys, {} bytes)", filePath, numKeys, offset);
        return offset;
    }

    private void flushDataBlock() throws IOException {
        long blockOffset = offset;
        byte[] blockData = dataBlockBuilder.finish();
        writeFile(blockData);

        // Add to index block
        indexBlockBuilder.add(lastKey, encodeBlockMeta(blockOffset, blockData.length));
        dataBlocks.add(new BlockMeta(blockOffset, blockData.length, lastKey));

        dataBlockBuilder.reset();
    }

    private void writeFile(byte[] data) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(data));
        }
        offset += data.length;
    }

    private byte[] encodeBlockMeta(long offset, int size) {
        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.putLong(offset);
        buf.putInt(size);
        return buf.array();
    }

    private void buildBloomFilter() {
        if (numKeys == 0) return;

        int bits = numKeys * bloomBitsPerKey;
        bits = Math.max(bits, 64);
        int numLongs = (bits + 63) / 64;
        bloomFilter = new long[numLongs];

        for (BlockMeta meta : dataBlocks) {
            int h = hash(meta.largestKey);
            for (int i = 0; i < 3; i++) {
                int bitPos = (h + i * 0x9e3779b9) % (numLongs * 64);
                if (bitPos < 0) bitPos = -bitPos;
                bloomFilter[bitPos / 64] |= (1L << (bitPos % 64));
            }
        }
    }

    private byte[] serializeBloomFilter() {
        if (bloomFilter == null) {
            return new byte[4]; // just numKeys=0
        }
        int size = 4 + bloomFilter.length * 8;
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(numKeys);
        for (long l : bloomFilter) {
            buf.putLong(l);
        }
        return buf.array();
    }

    private int hash(byte[] data) {
        int h = 0x811c9dc5;
        for (byte b : data) {
            h ^= Byte.toUnsignedInt(b);
            h *= 0x01000193;
        }
        return h;
    }

    record BlockMeta(long offset, int size, byte[] largestKey) {}

    /**
     * Byte comparator for SSTable key ordering.
     */
    public static class MemTableComparator {
        public static int compare(byte[] a, byte[] b) {
            int minLen = Math.min(a.length, b.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
                if (cmp != 0) return cmp;
            }
            return a.length - b.length;
        }
    }
}
