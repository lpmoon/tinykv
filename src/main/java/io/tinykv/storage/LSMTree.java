package io.tinykv.storage;

import io.tinykv.common.Config;
import io.tinykv.common.Record.RecordType;
import io.tinykv.storage.compaction.Compaction;
import io.tinykv.storage.memtable.MemTable;
import io.tinykv.storage.memtable.MemTableFactory;
import io.tinykv.storage.sstable.SSTableReader;
import io.tinykv.storage.wal.WAL;
import io.tinykv.storage.wal.WALEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSM-Tree based storage engine implementation with multiple memtables.
 *
 * Write path:  WAL -> MemTable -> (full) -> Immutable Queue -> (flush) -> SSTable
 * Read path:   MemTable -> Immutable MemTables (newest first) -> SSTable (L0 -> L1 -> ...)
 *
 * Inspired by RocksDB's memtable architecture.
 */
public class LSMTree implements StorageEngine {

    private static final Logger LOG = LoggerFactory.getLogger(LSMTree.class);

    private final Config config;
    private MemTable memTable;
    private final LinkedList<MemTableWithWal> immutableMemTables;
    private final WAL wal;
    private final Compaction compaction;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile boolean closed = false;

    /**
     * MemTable + its corresponding WAL sequence number.
     */
    private static class MemTableWithWal {
        final MemTable memTable;
        final long walSeq;

        MemTableWithWal(MemTable memTable, long walSeq) {
            this.memTable = memTable;
            this.walSeq = walSeq;
        }
    }

    public LSMTree(Config config) throws IOException {
        this.config = config;
        this.memTable = createNewMemTable();
        this.immutableMemTables = new LinkedList<>();
        this.wal = new WAL(config.getDataDir() + "/wal");
        this.compaction = new Compaction(config);
        this.compaction.start();
    }

    /**
     * Create a new memtable based on config.
     */
    private MemTable createNewMemTable() {
        if (config.getMemTableType() == null) {
            return MemTableFactory.create(io.tinykv.storage.memtable.MemTableType.SKIP_LIST);
        }
        return MemTableFactory.create(config.getMemTableType(), config.getHashBuckets());
    }

    @Override
    public void put(byte[] key, byte[] value) {
        lock.writeLock().lock();
        try {
            WALEntry entry = new WALEntry(RecordType.PUT, key, value);
            wal.append(entry);
            memTable.put(key, value);
            maybeSwitchMemTable();
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
            // 1. Check active MemTable (newest)
            byte[] value = memTable.get(key);
            if (value != null) {
                return Optional.of(value);
            }
            if (memTable.isDeleted(key)) {
                return Optional.empty();
            }

            // 2. Check Immutable MemTables (newest first, since they're added to head)
            for (MemTableWithWal mt : immutableMemTables) {
                value = mt.memTable.get(key);
                if (value != null) {
                    return Optional.of(value);
                }
                if (mt.memTable.isDeleted(key)) {
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
            maybeSwitchMemTable();
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
            maybeSwitchMemTable();
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

            // Active MemTable (newest)
            iterators.add(memTable.iterator(startKey, endKey));

            // Immutable MemTables (newest first)
            for (MemTableWithWal mt : immutableMemTables) {
                iterators.add(mt.memTable.iterator(startKey, endKey));
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
            // 1. Replay WAL entries (covers crash before flush: entries not yet in SSTable)
            List<WALEntry> entries = wal.recover();
            for (WALEntry entry : entries) {
                if (entry.type() == RecordType.PUT) {
                    memTable.put(entry.key(), entry.value());
                } else {
                    memTable.delete(entry.key());
                }
            }
            LOG.info("Recovered {} entries from WAL", entries.size());

            // 2. Load data from SSTable (covers clean shutdown: entries already flushed)
            for (SSTableReader reader : compaction.getAllReaders()) {
                int count = 0;
                try (KVIterator it = reader.iterator()) {
                    while (it.hasNext()) {
                        it.next();
                        byte[] key = it.key();
                        byte[] value = it.value();
                        memTable.put(key, value != null ? value : new byte[0]);
                        // Mark deletions explicitly
                        if (value == null) {
                            memTable.delete(key);
                        }
                        count++;
                    }
                }
                LOG.info("Recovered {} entries from SSTable reader", count);
            }
        } catch (IOException e) {
            throw new StorageException("Failed to recover storage engine", e);
        }
    }

    @Override
    public void flush() {
        lock.writeLock().lock();
        try {
            // Switch current memtable if not empty
            if (!memTable.isEmpty()) {
                switchMemTable();
            }
            // Flush all immutable memtables
            while (!immutableMemTables.isEmpty()) {
                flushOldestImmutable();
            }
        } catch (IOException e) {
            throw new StorageException("Failed to flush", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Check if we need to switch to a new memtable.
     */
    private void maybeSwitchMemTable() throws IOException {
        if (memTable.approximateSize() >= config.getMemTableSize()) {
            switchMemTable();
        }
    }

    /**
     * Switch current memtable to immutable and create new active memtable.
     * May block (write stall) if we hit max write buffer number limit.
     */
    private void switchMemTable() throws IOException {
        // Write stall: if too many immutable memtables, flush the oldest one first
        while (immutableMemTables.size() >= config.getMaxWriteBufferNumber() - 1) {
            LOG.warn("Write stall: {} immutable memtables waiting (max {}), flushing oldest",
                    immutableMemTables.size(), config.getMaxWriteBufferNumber() - 1);
            flushOldestImmutable();
        }

        if (memTable.isEmpty()) return;

        // Rotate WAL first
        long walSeq = wal.rotate();

        // Swap: current MemTable becomes immutable, create fresh MemTable
        MemTable oldMemTable = memTable;
        memTable = createNewMemTable();
        immutableMemTables.addFirst(new MemTableWithWal(oldMemTable, walSeq));

        LOG.info("Switched MemTable (approx {} bytes, {} entries), immutable queue size: {}",
                oldMemTable.approximateSize(), oldMemTable.entryCount(), immutableMemTables.size());
    }

    /**
     * Flush the oldest immutable memtable to SSTable (must hold write lock).
     */
    private void flushOldestImmutable() throws IOException {
        if (immutableMemTables.isEmpty()) {
            return;
        }

        MemTableWithWal toFlush = immutableMemTables.removeLast();

        LOG.info("Flushing immutable MemTable (approx {} bytes, {} entries)",
                toFlush.memTable.approximateSize(), toFlush.memTable.entryCount());

        // Flush the immutable MemTable to SSTable
        compaction.flushMemTable(toFlush.memTable);

        // Purge old WAL file now that its entries are safely in SSTable
        try {
            wal.purge(toFlush.walSeq);
        } catch (IOException e) {
            LOG.warn("Failed to purge old WAL file (seq {}): {}", toFlush.walSeq, e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        lock.writeLock().lock();
        IOException firstException = null;
        try {
            // Switch current memtable to immutable if not empty
            if (!memTable.isEmpty()) {
                try {
                    long walSeq = wal.rotate();
                    immutableMemTables.addFirst(new MemTableWithWal(memTable, walSeq));
                    memTable = createNewMemTable();
                } catch (IOException e) {
                    if (firstException == null) firstException = e;
                }
            }

            // Flush all immutable memtables
            while (!immutableMemTables.isEmpty()) {
                try {
                    flushOldestImmutable();
                } catch (IOException e) {
                    if (firstException == null) firstException = e;
                }
            }

            // Always mark as closed and clean up
            closed = true;
            try {
                wal.close();
            } catch (IOException e) {
                if (firstException == null) firstException = e;
            }
            compaction.stop();
        } finally {
            lock.writeLock().unlock();
        }
        if (firstException != null) {
            throw firstException;
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
