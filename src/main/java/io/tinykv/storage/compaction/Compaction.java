package io.tinykv.storage.compaction;

import io.tinykv.common.Config;
import io.tinykv.storage.memtable.MemTable;
import io.tinykv.storage.sstable.*;
import io.tinykv.storage.KVIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages SSTable levels and performs compaction.
 *
 * Level layout:
 *   Level 0: SSTables from MemTable flush (may overlap)
 *   Level 1+: SSTables are sorted by key range (non-overlapping within a level)
 *
 * Compaction strategy: Leveled compaction
 *   - When L0 has too many files, compact L0+L1 -> new L1
 *   - When Ln has too many bytes, compact overlapping Ln+Ln+1 -> new Ln+1
 */
public class Compaction {

    private static final Logger LOG = LoggerFactory.getLogger(Compaction.class);

    private final Config config;
    private final String sstableDir;
    private final AtomicLong sstSeq = new AtomicLong(0);
    private final List<List<SSTableMeta>> levels;
    private final List<List<SSTableReader>> readers;

    private final ExecutorService compactionExecutor;

    public Compaction(Config config) {
        this.config = config;
        this.sstableDir = config.getDataDir() + "/sstable";
        this.levels = new ArrayList<>();
        this.readers = new ArrayList<>();
        for (int i = 0; i < config.getMaxLevels(); i++) {
            levels.add(new CopyOnWriteArrayList<>());
            readers.add(new CopyOnWriteArrayList<>());
        }
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "compaction");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws IOException {
        Files.createDirectories(Paths.get(sstableDir));
        loadExistingSSTables();
    }

    /**
     * Flush an immutable MemTable to a new L0 SSTable.
     */
    public SSTableMeta flushMemTable(MemTable memTable) throws IOException {
        long seq = sstSeq.incrementAndGet();
        String fileName = String.format("sst-L0-%020d.sst", seq);
        Path path = Paths.get(sstableDir, fileName);

        SSTableBuilder builder = new SSTableBuilder(path, config.getSstableBlockSize(), config.getBloomFilterBitsPerKey());

        byte[] smallest = null;
        byte[] largest = null;

        KVIterator it = memTable.iterator();
        while (it.hasNext()) {
            it.next();
            byte[] key = it.key();
            byte[] value = it.value();
            if (smallest == null) smallest = key.clone();
            largest = key.clone();
            builder.add(key, value != null ? value : new byte[0]);
        }
        it.close();

        long fileSize = builder.finish();

        SSTableMeta meta = new SSTableMeta(0, fileName, smallest, largest, fileSize);
        levels.get(0).add(meta);

        SSTableReader reader = new SSTableReader(path);
        try {
            reader.open();
            readers.get(0).add(reader);
        } catch (IOException e) {
            LOG.warn("Failed to open newly flushed SSTable {}: {}", fileName, e.getMessage());
            // File is on disk but reader failed - WAL recovery will reconstruct data
            // Do not add to readers list
        }

        LOG.info("Flushed MemTable to SSTable: {} ({} bytes)", fileName, fileSize);

        maybeCompact();
        return meta;
    }

    /**
     * Get all SSTable readers for a given level.
     */
    public List<SSTableReader> getReaders(int level) {
        return Collections.unmodifiableList(readers.get(level));
    }

    /**
     * Get all levels' readers (L0 first, then L1, ...).
     */
    public List<SSTableReader> getAllReaders() {
        List<SSTableReader> all = new ArrayList<>();
        for (int i = 0; i < config.getMaxLevels(); i++) {
            all.addAll(readers.get(i));
        }
        return all;
    }

    /**
     * Check if compaction is needed and trigger it.
     */
    private void maybeCompact() {
        if (levels.get(0).size() >= config.getL0CompactionTrigger()) {
            compactionExecutor.submit(this::compactL0ToL1);
        }
    }

    private void compactL0ToL1() {
        try {
            LOG.info("Starting L0->L1 compaction");
            List<SSTableMeta> l0Files = new ArrayList<>(levels.get(0));
            if (l0Files.isEmpty()) return;

            // Collect L0 iterators
            List<KVIterator> l0Iterators = new ArrayList<>();
            for (SSTableReader r : readers.get(0)) {
                l0Iterators.add(r.iterator());
            }

            // Compute L0's key range (union of all L0 files)
            byte[] l0Start = l0Files.get(0).getSmallestKey();
            byte[] l0End = l0Files.get(0).getLargestKey();
            for (int i = 1; i < l0Files.size(); i++) {
                byte[] s = l0Files.get(i).getSmallestKey();
                byte[] e = l0Files.get(i).getLargestKey();
                if (SSTableBuilder.MemTableComparator.compare(s, l0Start) < 0) l0Start = s;
                if (SSTableBuilder.MemTableComparator.compare(e, l0End) > 0) l0End = e;
            }

            // Find overlapping L1 files only
            List<SSTableMeta> overlappingL1 = new ArrayList<>();
            for (SSTableMeta l1 : levels.get(1)) {
                if (l1.overlapsRange(l0Start, l0End)) {
                    overlappingL1.add(l1);
                }
            }

            // Merge all L0 + overlapping L1 into a sorted iterator
            List<KVIterator> allIterators = new ArrayList<>(l0Iterators);
            for (SSTableMeta l1 : overlappingL1) {
                int l1Idx = findReaderIndex(1, l1.getFileName());
                if (l1Idx >= 0) {
                    allIterators.add(readers.get(1).get(l1Idx).iterator());
                }
            }

            MergeIterator mergeIt = new MergeIterator(allIterators);

            // Write multiple L1 files, each within target size
            long targetSize = getL1TargetSize();
            long seq = sstSeq.incrementAndGet();
            int fileIdx = 0;

            byte[] smallest = null;
            byte[] largest = null;
            long currentSize = 0;
            List<PendingEntry> entries = new ArrayList<>();

            while (mergeIt.hasNext()) {
                mergeIt.next();
                byte[] key = mergeIt.key();
                byte[] value = mergeIt.value();
                long entrySize = key.length + value.length;

                if (smallest == null) {
                    smallest = key.clone();
                } else if (currentSize + entrySize > targetSize) {
                    // Write current SSTable
                    writeL1SSTable(seq, fileIdx++, smallest, largest, entries);

                    // Reset for next SSTable
                    smallest = key.clone();
                    largest = null;
                    currentSize = 0;
                    entries = new ArrayList<>();
                }

                entries.add(new PendingEntry(key, value));
                largest = key.clone();
                currentSize += entrySize;
            }

            // Write last SSTable
            if (smallest != null) {
                writeL1SSTable(seq, fileIdx, smallest, largest, entries);
            }

            // Remove old L0 files
            for (SSTableMeta old : l0Files) {
                removeSSTable(old, 0);
            }
            // Remove old overlapping L1 files
            for (SSTableMeta old : overlappingL1) {
                removeSSTable(old, 1);
            }

            LOG.info("L0->L1 compaction complete: {} L1 files produced", fileIdx + 1);

        } catch (Exception e) {
            LOG.error("L0->L1 compaction failed", e);
        }
    }

    private long getL1TargetSize() {
        // L1 target: 10MB * levelSizeMultiplier
        return 10 * 1024 * 1024L * config.getLevelSizeMultiplier();
    }

    private void writeL1SSTable(long seq, int fileIdx, byte[] smallest, byte[] largest, List<PendingEntry> entries) throws IOException {
        if (entries.isEmpty()) return;

        String fileName = String.format("sst-L1-%020d-%03d.sst", seq, fileIdx);
        Path path = Paths.get(sstableDir, fileName);
        SSTableBuilder builder = new SSTableBuilder(path, config.getSstableBlockSize(), config.getBloomFilterBitsPerKey());

        for (PendingEntry e : entries) {
            builder.add(e.key, e.value);
        }
        long fileSize = builder.finish();

        SSTableMeta meta = new SSTableMeta(1, fileName, smallest, largest, fileSize);
        levels.get(1).add(meta);

        try {
            SSTableReader reader = new SSTableReader(path);
            reader.open();
            readers.get(1).add(reader);
        } catch (IOException e) {
            LOG.warn("Failed to open new SSTable {}: {}", fileName, e.getMessage());
        }

        LOG.debug("Wrote L1 SSTable: {} ({} bytes, {} entries)", fileName, fileSize, entries.size());
    }

    private void removeSSTable(SSTableMeta meta, int level) {
        levels.get(level).remove(meta);
        int idx = findReaderIndex(level, meta.getFileName());
        if (idx >= 0) {
            try {
                readers.get(level).get(idx).close();
            } catch (IOException ignored) {}
            readers.get(level).remove(idx);
        }
        try {
            Files.deleteIfExists(Paths.get(sstableDir, meta.getFileName()));
        } catch (IOException ignored) {}
    }

    private int findReaderIndex(int level, String fileName) {
        List<SSTableReader> rdrs = readers.get(level);
        for (int i = 0; i < rdrs.size(); i++) {
            if (rdrs.get(i).getFilePath().getFileName().toString().equals(fileName)) {
                return i;
            }
        }
        return -1;
    }

    private void loadExistingSSTables() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(sstableDir), "sst-*.sst")) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                int level = 0;
                if (name.contains("-L1-")) level = 1;
                else if (name.contains("-L2-")) level = 2;

                SSTableReader reader = new SSTableReader(p);
                try {
                    reader.open();
                } catch (IOException e) {
                    LOG.warn("Failed to open SSTable {} (level {}), skipping: {}", name, level, e.getMessage());
                    continue;
                }

                // We need smallest/largest keys from the index
                byte[] smallest = null;
                byte[] largest = null;
                if (!reader.isIndexEmpty()) {
                    smallest = reader.getIndexFirstKey();
                    largest = reader.getIndexLastKey();
                }

                SSTableMeta meta = new SSTableMeta(level, name, smallest, largest, Files.size(p));
                levels.get(level).add(meta);
                readers.get(level).add(reader);

                LOG.info("Loaded existing SSTable: {} (level {})", name, level);
            }
        }
    }

    /**
     * A key-value entry pending to be written to an SSTable.
     */
    private static class PendingEntry {
        final byte[] key;
        final byte[] value;
        PendingEntry(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Merge multiple sorted iterators into one.
     */
    static class MergeIterator implements KVIterator {
        private final PriorityQueue<IterEntry> heap;
        private byte[] currentKey;
        private byte[] currentValue;
        private boolean hasCurrent;

        private static class IterEntry implements Comparable<IterEntry> {
            KVIterator iter;
            byte[] key;
            byte[] value;

            IterEntry(KVIterator iter, byte[] key, byte[] value) {
                this.iter = iter;
                this.key = key;
                this.value = value;
            }

            @Override
            public int compareTo(IterEntry o) {
                return SSTableBuilder.MemTableComparator.compare(this.key, o.key);
            }
        }

        MergeIterator(List<KVIterator> iterators) {
            this.heap = new PriorityQueue<>();
            for (KVIterator it : iterators) {
                if (it.hasNext()) {
                    it.next();
                    heap.add(new IterEntry(it, it.key(), it.value()));
                }
            }
            hasCurrent = false;
        }

        @Override
        public boolean hasNext() {
            if (!hasCurrent && !heap.isEmpty()) {
                IterEntry e = heap.poll();
                currentKey = e.key;
                currentValue = e.value;
                hasCurrent = true;

                if (e.iter.hasNext()) {
                    e.iter.next();
                    heap.add(new IterEntry(e.iter, e.iter.key(), e.iter.value()));
                }
            }
            return hasCurrent;
        }

        @Override
        public void next() {
            hasCurrent = false;
        }

        @Override
        public byte[] key() {
            return currentKey;
        }

        @Override
        public byte[] value() {
            return currentValue;
        }

        @Override
        public void seek(byte[] target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekToFirst() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    public void stop() {
        compactionExecutor.shutdown();
        try {
            if (!compactionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
        }
        // Close all SSTableReader instances to release file handles
        for (List<SSTableReader> levelReaders : readers) {
            for (SSTableReader r : levelReaders) {
                try { r.close(); } catch (IOException ignored) {}
            }
        }
    }
}
