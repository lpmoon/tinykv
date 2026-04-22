package io.tinykv.storage.sstable;

import io.tinykv.storage.KVIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.NoSuchElementException;

/**
 * Reads an SSTable file. Provides point lookups and range scans.
 */
public class SSTableReader {

    private static final Logger LOG = LoggerFactory.getLogger(SSTableReader.class);
    private static final int MAGIC_NUMBER = 0x54494E59;
    private static final int FOOTER_SIZE = 40;

    private final Path filePath;
    private FileChannel channel;

    // Index block entries: largestKey -> (offset, size)
    private final TreeMap<byte[], BlockMeta> index = new TreeMap<>(SSTableBuilder.MemTableComparator::compare);

    // Bloom filter
    private long[] bloomFilter;
    private int numKeys;

    public SSTableReader(Path filePath) {
        this.filePath = filePath;
    }

    public void open() throws IOException {
        channel = FileChannel.open(filePath, StandardOpenOption.READ);
        readFooter();
        readIndexBlock();
        readMetaBlock();
    }

    public Optional<byte[]> get(byte[] key) {
        // Bloom filter check
        if (!mightContain(key)) {
            return Optional.empty();
        }

        // Find the block that may contain this key
        Map.Entry<byte[], BlockMeta> entry = index.floorEntry(key);
        if (entry == null) {
            return Optional.empty();
        }

        BlockMeta meta = entry.getValue();
        try {
            BlockData block = readBlock(meta);
            return block.get(key);
        } catch (IOException e) {
            LOG.error("Error reading block from {}", filePath, e);
            return Optional.empty();
        }
    }

    public KVIterator iterator() {
        return new SSTableIterator(this, index.values());
    }

    public KVIterator iterator(byte[] startKey, byte[] endKey) {
        Collection<BlockMeta> blocks;
        if (startKey != null) {
            Map.Entry<byte[], BlockMeta> floor = index.floorEntry(startKey);
            if (floor == null) {
                blocks = index.values();
            } else {
                blocks = index.tailMap(floor.getKey(), true).values();
            }
        } else {
            blocks = index.values();
        }
        return new SSTableIterator(this, blocks, startKey, endKey);
    }

    private void readFooter() throws IOException {
        long fileSize = channel.size();
        if (fileSize < FOOTER_SIZE) {
            throw new IOException("SSTable file too small: " + filePath);
        }

        ByteBuffer footerBuf = ByteBuffer.allocate(FOOTER_SIZE);
        channel.read(footerBuf, fileSize - FOOTER_SIZE);
        footerBuf.flip();

        long indexOffset = footerBuf.getLong();
        long indexSize = footerBuf.getLong();
        long metaOffset = footerBuf.getLong();
        long metaSize = footerBuf.getLong();
        int magic = footerBuf.getInt();

        if (magic != MAGIC_NUMBER) {
            throw new IOException("Invalid SSTable magic number in " + filePath);
        }

        // Read index block
        ByteBuffer indexBuf = ByteBuffer.allocate((int) indexSize);
        channel.read(indexBuf, indexOffset);
        indexBuf.flip();
        parseIndexBlock(indexBuf);

        // Read meta block (bloom filter)
        ByteBuffer metaBuf = ByteBuffer.allocate((int) metaSize);
        channel.read(metaBuf, metaOffset);
        metaBuf.flip();
        parseMetaBlock(metaBuf);
    }

    private void parseIndexBlock(ByteBuffer buf) {
        index.clear();
        while (buf.remaining() > 12) {
            int sharedLen = buf.getInt();
            int unsharedLen = buf.getInt();
            int valueLen = buf.getInt();
            byte[] key = new byte[sharedLen + unsharedLen]; // for index entries, shared is always 0
            buf.get(key, sharedLen, unsharedLen);
            byte[] meta = new byte[valueLen];
            buf.get(meta);
            ByteBuffer metaBuf = ByteBuffer.wrap(meta);
            long offset = metaBuf.getLong();
            int size = metaBuf.getInt();
            index.put(key, new BlockMeta(offset, size, key));
        }
    }

    private void parseMetaBlock(ByteBuffer buf) {
        if (buf.remaining() < 4) return;
        numKeys = buf.getInt();
        if (buf.remaining() >= 8) {
            int numLongs = buf.remaining() / 8;
            bloomFilter = new long[numLongs];
            for (int i = 0; i < numLongs; i++) {
                bloomFilter[i] = buf.getLong();
            }
        }
    }

    private void readIndexBlock() {
        // Already read in readFooter
    }

    private void readMetaBlock() {
        // Already read in readFooter
    }

    private boolean mightContain(byte[] key) {
        if (bloomFilter == null || bloomFilter.length == 0) return true;
        int h = hash(key);
        int numBits = bloomFilter.length * 64;
        for (int i = 0; i < 3; i++) {
            int bitPos = (h + i * 0x9e3779b9) % numBits;
            if (bitPos < 0) bitPos = -bitPos;
            if ((bloomFilter[bitPos / 64] & (1L << (bitPos % 64))) == 0) {
                return false;
            }
        }
        return true;
    }

    private BlockData readBlock(BlockMeta meta) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(meta.size);
        channel.read(buf, meta.offset);
        buf.flip();
        return new BlockData(buf);
    }

    private int hash(byte[] data) {
        int h = 0x811c9dc5;
        for (byte b : data) {
            h ^= Byte.toUnsignedInt(b);
            h *= 0x01000193;
        }
        return h;
    }

    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    public Path getFilePath() {
        return filePath;
    }

    public boolean isIndexEmpty() {
        return index.isEmpty();
    }

    public byte[] getIndexFirstKey() {
        return index.isEmpty() ? null : index.firstKey();
    }

    public byte[] getIndexLastKey() {
        return index.isEmpty() ? null : index.lastKey();
    }

    public long getFileSize() throws IOException {
        return channel != null ? channel.size() : 0;
    }

    /**
     * Parsed block meta from the index.
     */
    record BlockMeta(long offset, int size, byte[] largestKey) {}

    /**
     * In-memory representation of a data block for reading.
     */
    static class BlockData {
        private final List<byte[]> keys = new ArrayList<>();
        private final List<byte[]> values = new ArrayList<>();

        BlockData(ByteBuffer buf) {
            byte[] prevKey = new byte[0];
            while (buf.remaining() > 0) {
                if (buf.remaining() < 12) break; // not enough for header

                int sharedLen = buf.getInt();
                int unsharedLen = buf.getInt();
                int valueLen = buf.getInt();

                if (buf.remaining() < unsharedLen + valueLen) break;

                byte[] key = new byte[sharedLen + unsharedLen];
                System.arraycopy(prevKey, 0, key, 0, sharedLen);
                buf.get(key, sharedLen, unsharedLen);

                byte[] value = new byte[valueLen];
                if (valueLen > 0) {
                    buf.get(value);
                }

                keys.add(key);
                values.add(value);
                prevKey = key;
            }
        }

        Optional<byte[]> get(byte[] target) {
            for (int i = 0; i < keys.size(); i++) {
                int cmp = SSTableBuilder.MemTableComparator.compare(keys.get(i), target);
                if (cmp == 0) {
                    byte[] val = values.get(i);
                    return val.length == 0 ? Optional.empty() : Optional.of(val);
                }
                if (cmp > 0) break;
            }
            return Optional.empty();
        }

        List<byte[]> getKeys() { return keys; }
        List<byte[]> getValues() { return values; }
    }

    /**
     * Iterator over an SSTable, optionally bounded by start/end keys.
     */
    static class SSTableIterator implements KVIterator {

        private final SSTableReader reader;
        private final Collection<BlockMeta> blocks;
        private final byte[] startKey;
        private final byte[] endKey;

        private java.util.Iterator<BlockMeta> blockIt;
        private BlockData currentBlock;
        private int entryIndex;
        private boolean initialized;

        // Current valid entry - only valid after successful next()
        private byte[] currentKey;
        private byte[] currentValue;
        private boolean hasCurrent;

        SSTableIterator(SSTableReader reader, Collection<BlockMeta> blocks) {
            this(reader, blocks, null, null);
        }

        SSTableIterator(SSTableReader reader, Collection<BlockMeta> blocks, byte[] startKey, byte[] endKey) {
            this.reader = reader;
            this.blocks = blocks;
            this.startKey = startKey;
            this.endKey = endKey;
            this.blockIt = blocks.iterator();
            this.entryIndex = 0;
            this.initialized = false;
            this.hasCurrent = false;
        }

        @Override
        public boolean hasNext() {
            if (!initialized) {
                advanceToStart();
                initialized = true;
            }
            // Check if we have more entries in current block
            if (currentBlock != null && entryIndex < currentBlock.getKeys().size()) {
                byte[] key = currentBlock.getKeys().get(entryIndex);
                // Check end key bound
                if (endKey != null && SSTableBuilder.MemTableComparator.compare(key, endKey) > 0) {
                    return false;
                }
                return true;
            }
            // Try to advance to next block
            advanceBlock();
            return currentBlock != null && entryIndex < currentBlock.getKeys().size();
        }

        @Override
        public void next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            // Save the current entry
            currentKey = currentBlock.getKeys().get(entryIndex);
            currentValue = currentBlock.getValues().get(entryIndex);
            hasCurrent = true;

            // Advance for next time (but don't advance block yet)
            entryIndex++;
        }

        @Override
        public byte[] key() {
            if (!hasCurrent) {
                throw new NoSuchElementException();
            }
            return currentKey;
        }

        @Override
        public byte[] value() {
            if (!hasCurrent) {
                throw new NoSuchElementException();
            }
            return currentValue;
        }

        @Override
        public void seek(byte[] target) {
            this.blockIt = reader.index.tailMap(target, true).values().iterator();
            this.entryIndex = 0;
            this.currentBlock = null;
            this.initialized = false;
            this.hasCurrent = false;
            this.currentKey = null;
            this.currentValue = null;
        }

        @Override
        public void seekToFirst() {
            this.blockIt = reader.index.values().iterator();
            this.entryIndex = 0;
            this.currentBlock = null;
            this.initialized = false;
            this.hasCurrent = false;
            this.currentKey = null;
            this.currentValue = null;
        }

        @Override
        public void close() {
            // no-op
        }

        private void advanceToStart() {
            while (blockIt.hasNext()) {
                try {
                    currentBlock = reader.readBlock(blockIt.next());
                    entryIndex = 0;
                    if (startKey != null) {
                        while (entryIndex < currentBlock.getKeys().size()) {
                            if (SSTableBuilder.MemTableComparator.compare(
                                    currentBlock.getKeys().get(entryIndex), startKey) >= 0) {
                                break;
                            }
                            entryIndex++;
                        }
                    }
                    if (entryIndex < currentBlock.getKeys().size()) {
                        return;
                    }
                } catch (IOException e) {
                    return;
                }
            }
            currentBlock = null;
        }

        private void advanceBlock() {
            while (blockIt.hasNext()) {
                try {
                    currentBlock = reader.readBlock(blockIt.next());
                    entryIndex = 0;
                    if (currentBlock.getKeys().size() > 0) {
                        return;
                    }
                } catch (IOException e) {
                    return;
                }
            }
            currentBlock = null;
        }
    }
}
