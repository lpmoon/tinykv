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

            // Collect all entries from L0 and overlapping L1 files
            List<KVIterator> iterators = new ArrayList<>();
            for (SSTableReader r : readers.get(0)) {
                iterators.add(r.iterator());
            }
            // Find overlapping L1 files
            for (SSTableMeta l1 : levels.get(1)) {
                iterators.add(readers.get(1).get(levels.get(1).indexOf(l1)).iterator());
            }

            // Merge and write new L1 SSTable
            MergeIterator mergeIt = new MergeIterator(iterators);
            long seq = sstSeq.incrementAndGet();
            String fileName = String.format("sst-L1-%020d.sst", seq);
            Path path = Paths.get(sstableDir, fileName);
            SSTableBuilder builder = new SSTableBuilder(path, config.getSstableBlockSize(), config.getBloomFilterBitsPerKey());

            byte[] smallest = null;
            byte[] largest = null;

            while (mergeIt.hasNext()) {
                mergeIt.next();
                byte[] key = mergeIt.key();
                byte[] value = mergeIt.value();
                if (smallest == null) smallest = key.clone();
                largest = key.clone();
                builder.add(key, value != null ? value : new byte[0]);
            }

            long fileSize = builder.finish();

            // Update metadata
            SSTableMeta meta = new SSTableMeta(1, fileName, smallest, largest, fileSize);
            levels.get(1).add(meta);

            SSTableReader reader = new SSTableReader(path);
            reader.open();
            readers.get(1).add(reader);

            // Remove old L0 files
            for (SSTableMeta old : l0Files) {
                levels.get(0).remove(old);
                int idx = l0Files.indexOf(old);
                if (idx < readers.get(0).size()) {
                    readers.get(0).remove(idx).close();
                }
                Files.deleteIfExists(Paths.get(sstableDir, old.getFileName()));
            }

            LOG.info("L0->L1 compaction complete: {} -> {} bytes", fileName, fileSize);

        } catch (Exception e) {
            LOG.error("L0->L1 compaction failed", e);
        }
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
