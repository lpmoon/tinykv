package io.tinykv.storage;

import io.tinykv.common.Config;
import io.tinykv.common.Record.RecordType;
import io.tinykv.storage.compaction.Compaction;
import io.tinykv.storage.memtable.MemTable;
import io.tinykv.storage.sstable.SSTableReader;
import io.tinykv.storage.wal.WAL;
import io.tinykv.storage.wal.WALEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSM-Tree based storage engine implementation.
 *
 * Write path:  WAL -> MemTable -> (flush) -> SSTable
 * Read path:   MemTable -> Immutable MemTable -> SSTable (L0 -> L1 -> ...)
 * Scan path:   Merge iterator across all levels
 */
public class LSMTree implements StorageEngine {

    private static final Logger LOG = LoggerFactory.getLogger(LSMTree.class);

    private final Config config;
    private MemTable memTable;
    private MemTable immutableMemTable;
    private final WAL wal;
    private final Compaction compaction;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile boolean closed = false;

    public LSMTree(Config config) throws IOException {
        this.config = config;
        this.memTable = new MemTable();
        this.immutableMemTable = null;
        this.wal = new WAL(config.getDataDir() + "/wal");
        this.compaction = new Compaction(config);
        this.compaction.start();
    }

    @Override
    public void put(byte[] key, byte[] value) {
        lock.writeLock().lock();
        try {
            WALEntry entry = new WALEntry(RecordType.PUT, key, value);
            wal.append(entry);
            memTable.put(key, value);
            maybeFlush();
        } catch (IOException e) {
            throw new StorageException("Failed to put key", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        lock.readLock().lock();
        try {
            // 1. Check MemTable
            byte[] value = memTable.get(key);
            if (value != null) {
                return Optional.of(value);
            }
            if (memTable.isDeleted(key)) {
                return Optional.empty();
            }

            // 2. Check Immutable MemTable
            if (immutableMemTable != null) {
                value = immutableMemTable.get(key);
                if (value != null) {
                    return Optional.of(value);
                }
                if (immutableMemTable.isDeleted(key)) {
                    return Optional.empty();
                }
            }

            // 3. Check SSTables (L0 first, then L1, ...)
            for (SSTableReader reader : compaction.getAllReaders()) {
                Optional<byte[]> result = reader.get(key);
                if (result.isPresent()) {
                    return result;
                }
            }

            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void delete(byte[] key) {
        lock.writeLock().lock();
        try {
            WALEntry entry = new WALEntry(RecordType.DELETE, key, null);
            wal.append(entry);
            memTable.delete(key);
            maybeFlush();
        } catch (IOException e) {
            throw new StorageException("Failed to delete key", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void write(Batch batch) {
        lock.writeLock().lock();
        try {
            List<WALEntry> walEntries = new ArrayList<>();
            for (Batch.BatchEntry e : batch.getEntries()) {
                walEntries.add(new WALEntry(e.type(), e.key(), e.value()));
            }
            wal.appendBatch(walEntries);

            for (Batch.BatchEntry e : batch.getEntries()) {
                if (e.type() == RecordType.PUT) {
                    memTable.put(e.key(), e.value());
                } else {
                    memTable.delete(e.key());
                }
            }
            maybeFlush();
        } catch (IOException e) {
            throw new StorageException("Failed to write batch", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public KVIterator scan(byte[] startKey, byte[] endKey) {
        lock.readLock().lock();
        try {
            List<KVIterator> iterators = new ArrayList<>();

            // MemTable
            iterators.add(memTable.iterator(startKey, endKey));

            // Immutable MemTable
            if (immutableMemTable != null) {
                iterators.add(immutableMemTable.iterator(startKey, endKey));
            }

            // SSTables
            for (SSTableReader reader : compaction.getAllReaders()) {
                iterators.add(reader.iterator(startKey, endKey));
            }

            return new MergingIterator(iterators);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void recover() {
        try {
            List<WALEntry> entries = wal.recover();
            for (WALEntry entry : entries) {
                if (entry.type() == RecordType.PUT) {
                    memTable.put(entry.key(), entry.value());
                } else {
                    memTable.delete(entry.key());
                }
            }
            LOG.info("Recovered {} entries from WAL", entries.size());
        } catch (IOException e) {
            throw new StorageException("Failed to recover from WAL", e);
        }
    }

    @Override
    public void flush() {
        lock.writeLock().lock();
        try {
            doFlush();
        } catch (IOException e) {
            throw new StorageException("Failed to flush MemTable", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void maybeFlush() throws IOException {
        if (memTable.approximateSize() >= config.getMemTableSize()) {
            doFlush();
        }
    }

    private void doFlush() throws IOException {
        if (memTable.isEmpty()) return;

        // Swap: current MemTable becomes immutable, create fresh MemTable
        immutableMemTable = memTable;
        memTable = new MemTable();

        LOG.info("Flushing MemTable (approx {} bytes, {} entries)",
                immutableMemTable.approximateSize(), immutableMemTable.entryCount());

        // Flush the immutable MemTable to SSTable
        compaction.flushMemTable(immutableMemTable);

        // Rotate WAL
        wal.rotate();

        // Clear immutable reference
        immutableMemTable = null;
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        lock.writeLock().lock();
        try {
            if (!memTable.isEmpty()) {
                doFlush();
            }
            wal.close();
            compaction.stop();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Merges multiple KVIterators, deduplicating by key (newest wins).
     */
    static class MergingIterator implements KVIterator {

        private final List<KVIterator> iterators;
        private final PriorityQueue<IterEntry> heap;
        private byte[] currentKey;
        private byte[] currentValue;
        private boolean hasCurrent;

        private static class IterEntry implements Comparable<IterEntry> {
            final int sourceIndex;
            final KVIterator iter;
            final byte[] key;
            final byte[] value;

            IterEntry(int sourceIndex, KVIterator iter, byte[] key, byte[] value) {
                this.sourceIndex = sourceIndex;
                this.iter = iter;
                this.key = key;
                this.value = value;
            }

            @Override
            public int compareTo(IterEntry o) {
                int cmp = MemTable.compareBytes(this.key, o.key);
                if (cmp != 0) return cmp;
                // Newer sources (lower index) win on ties
                return Integer.compare(this.sourceIndex, o.sourceIndex);
            }
        }

        MergingIterator(List<KVIterator> iterators) {
            this.iterators = iterators;
            this.heap = new PriorityQueue<>();
            for (int i = 0; i < iterators.size(); i++) {
                KVIterator it = iterators.get(i);
                if (it.hasNext()) {
                    it.next();
                    heap.add(new IterEntry(i, it, it.key(), it.value()));
                }
            }
            hasCurrent = false;
        }

        @Override
        public boolean hasNext() {
            if (!hasCurrent) {
                advance();
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
            heap.clear();
            for (int i = 0; i < iterators.size(); i++) {
                KVIterator it = iterators.get(i);
                it.seek(target);
                if (it.hasNext()) {
                    it.next();
                    heap.add(new IterEntry(i, it, it.key(), it.value()));
                }
            }
            hasCurrent = false;
        }

        @Override
        public void seekToFirst() {
            heap.clear();
            for (int i = 0; i < iterators.size(); i++) {
                KVIterator it = iterators.get(i);
                it.seekToFirst();
                if (it.hasNext()) {
                    it.next();
                    heap.add(new IterEntry(i, it, it.key(), it.value()));
                }
            }
            hasCurrent = false;
        }

        @Override
        public void close() {
            for (KVIterator it : iterators) {
                try { it.close(); } catch (Exception ignored) {}
            }
        }

        private void advance() {
            while (!heap.isEmpty()) {
                IterEntry top = heap.poll();
                byte[] key = top.key;

                // Skip duplicates: if we already returned this key, skip
                if (hasCurrent && MemTable.compareBytes(key, currentKey) == 0) {
                    refill(top);
                    continue;
                }

                currentKey = key;
                currentValue = top.value;
                hasCurrent = true;

                // Skip any remaining entries with the same key (older versions)
                refill(top);
                while (!heap.isEmpty() && MemTable.compareBytes(heap.peek().key, currentKey) == 0) {
                    IterEntry dup = heap.poll();
                    refill(dup);
                }

                // Skip tombstones (null value means deleted)
                if (currentValue == null) {
                    hasCurrent = false;
                    continue;
                }
                return;
            }
            hasCurrent = false;
        }

        private void refill(IterEntry entry) {
            if (entry.iter.hasNext()) {
                entry.iter.next();
                heap.add(new IterEntry(entry.sourceIndex, entry.iter, entry.iter.key(), entry.iter.value()));
            }
        }
    }

    /**
     * Runtime exception for storage errors.
     */
    public static class StorageException extends RuntimeException {
        public StorageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
