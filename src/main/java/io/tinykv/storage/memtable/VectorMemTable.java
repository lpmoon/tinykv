package io.tinykv.storage.memtable;

import io.tinykv.storage.KVIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory KV store backed by sorted array list.
 *
 * Features:
 * - Writes are O(n) but low constant factor (unsorted first)
 * - Reads are O(log n) after sorting
 * - Best for: bulk load, write-once-read-many workloads
 *
 * Note: This implementation is simplified - for simplicity, we sort on first read.
 */
public class VectorMemTable implements MemTable {

    private static final byte[] TOMBSTONE = new byte[0];

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<Entry> buffer;     // Unsorted write buffer
    private List<Entry> sortedEntries;    // Sorted for reads
    private long approximateSize;

    private static class Entry {
        final byte[] key;
        final byte[] value;

        Entry(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    public VectorMemTable() {
        this.buffer = new ArrayList<>();
        this.sortedEntries = null;
        this.approximateSize = 0;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        lock.writeLock().lock();
        try {
            buffer.add(new Entry(key, value));
            approximateSize += key.length + value.length;
            // Invalidate sorted view
            sortedEntries = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(byte[] key) {
        lock.writeLock().lock();
        try {
            buffer.add(new Entry(key, TOMBSTONE));
            approximateSize += key.length;
            sortedEntries = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void ensureSorted() {
        if (sortedEntries == null) {
            // Merge and sort all entries (with dedup: keep last write)
            java.util.TreeMap<byte[], Entry> tmp = new java.util.TreeMap<>(MemTable::compareBytes);
            for (Entry e : buffer) {
                tmp.put(e.key, e);
            }
            sortedEntries = new ArrayList<>(tmp.values());
        }
    }

    @Override
    public byte[] get(byte[] key) {
        lock.readLock().lock();
        try {
            ensureSorted();
            // Binary search
            int low = 0, high = sortedEntries.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                Entry e = sortedEntries.get(mid);
                int cmp = MemTable.compareBytes(key, e.key);
                if (cmp < 0) {
                    high = mid - 1;
                } else if (cmp > 0) {
                    low = mid + 1;
                } else {
                    return e.value == TOMBSTONE ? null : e.value;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean contains(byte[] key) {
        byte[] value = get(key);
        return value != null;
    }

    @Override
    public boolean isDeleted(byte[] key) {
        lock.readLock().lock();
        try {
            ensureSorted();
            int low = 0, high = sortedEntries.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                Entry e = sortedEntries.get(mid);
                int cmp = MemTable.compareBytes(key, e.key);
                if (cmp < 0) {
                    high = mid - 1;
                } else if (cmp > 0) {
                    low = mid + 1;
                } else {
                    return e.value == TOMBSTONE;
                }
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long approximateSize() {
        return approximateSize;
    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    @Override
    public int entryCount() {
        // Return count of unique keys
        lock.readLock().lock();
        try {
            ensureSorted();
            return sortedEntries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public KVIterator iterator() {
        return iterator(null, null);
    }

    @Override
    public KVIterator iterator(byte[] startKey, byte[] endKey) {
        lock.readLock().lock();
        try {
            ensureSorted();
            return new VectorMemTableIterator(new ArrayList<>(sortedEntries), startKey, endKey);
        } finally {
            lock.readLock().unlock();
        }
    }

    private static class VectorMemTableIterator implements KVIterator {

        private final List<Entry> entries;
        private final byte[] startKey;
        private final byte[] endKey;
        private int currentIndex;
        private byte[] currentKey;
        private byte[] currentValue;

        VectorMemTableIterator(List<Entry> entries, byte[] startKey, byte[] endKey) {
            this.entries = entries;
            this.startKey = startKey;
            this.endKey = endKey;
            this.currentIndex = 0;

            // Seek to start key
            if (startKey != null) {
                while (currentIndex < entries.size()) {
                    int cmp = MemTable.compareBytes(entries.get(currentIndex).key, startKey);
                    if (cmp >= 0) break;
                    currentIndex++;
                }
            }

            // Apply end key filter in hasNext
            this.currentKey = null;
            this.currentValue = null;
        }

        @Override
        public boolean hasNext() {
            if (currentIndex >= entries.size()) {
                return false;
            }
            if (endKey != null) {
                int cmp = MemTable.compareBytes(entries.get(currentIndex).key, endKey);
                if (cmp >= 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void next() {
            Entry e = entries.get(currentIndex);
            currentKey = e.key;
            currentValue = e.value == TOMBSTONE ? null : e.value;
            currentIndex++;
        }

        @Override
        public byte[] key() {
            if (currentKey == null) throw new IllegalStateException("Call next() first");
            return currentKey;
        }

        @Override
        public byte[] value() {
            if (currentKey == null) throw new IllegalStateException("Call next() first");
            return currentValue;
        }

        @Override
        public void seek(byte[] target) {
            currentIndex = 0;
            while (currentIndex < entries.size()) {
                int cmp = MemTable.compareBytes(entries.get(currentIndex).key, target);
                if (cmp >= 0) break;
                currentIndex++;
            }
            currentKey = null;
            currentValue = null;
        }

        @Override
        public void seekToFirst() {
            currentIndex = 0;
            currentKey = null;
            currentValue = null;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
