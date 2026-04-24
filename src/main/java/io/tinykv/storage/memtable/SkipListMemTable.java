package io.tinykv.storage.memtable;

import io.tinykv.storage.KVIterator;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory sorted KV store backed by ConcurrentSkipListMap.
 * Thread-safe for concurrent reads and writes.
 *
 * Features: O(log n) point reads/range scans, good for general purpose.
 */
public class SkipListMemTable implements MemTable {

    private static final byte[] TOMBSTONE = new byte[0];

    private final ConcurrentSkipListMap<byte[], byte[]> table;
    private long approximateSize;

    public SkipListMemTable() {
        this.table = new ConcurrentSkipListMap<>(MemTable::compareBytes);
        this.approximateSize = 0;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        byte[] prev = table.put(key, value);
        if (prev == null) {
            approximateSize += key.length + value.length;
        } else {
            approximateSize += value.length - prev.length;
        }
    }

    @Override
    public void delete(byte[] key) {
        byte[] prev = table.put(key, TOMBSTONE);
        if (prev == null) {
            approximateSize += key.length;
        }
    }

    @Override
    public byte[] get(byte[] key) {
        byte[] value = table.get(key);
        if (value == null || value == TOMBSTONE) {
            return null;
        }
        return value;
    }

    @Override
    public boolean contains(byte[] key) {
        byte[] value = table.get(key);
        return value != null && value != TOMBSTONE;
    }

    @Override
    public boolean isDeleted(byte[] key) {
        byte[] value = table.get(key);
        return value == TOMBSTONE;
    }

    @Override
    public long approximateSize() {
        return approximateSize;
    }

    @Override
    public boolean isEmpty() {
        return table.isEmpty();
    }

    @Override
    public int entryCount() {
        return table.size();
    }

    @Override
    public KVIterator iterator() {
        return new SkipListMemTableIterator(table);
    }

    @Override
    public KVIterator iterator(byte[] startKey, byte[] endKey) {
        ConcurrentNavigableMap<byte[], byte[]> subMap;
        if (startKey != null && endKey != null) {
            // If startKey >= endKey, return empty iterator
            if (MemTable.compareBytes(startKey, endKey) >= 0) {
                return new SkipListMemTableIterator(new ConcurrentSkipListMap<>(MemTable::compareBytes));
            }
            subMap = table.subMap(startKey, true, endKey, false);
        } else if (startKey != null) {
            subMap = table.tailMap(startKey, true);
        } else if (endKey != null) {
            subMap = table.headMap(endKey, false);
        } else {
            subMap = table;
        }
        return new SkipListMemTableIterator(subMap);
    }

    /**
     * MemTable iterator wrapping the skip list map.
     */
    private static class SkipListMemTableIterator implements KVIterator {

        private final ConcurrentNavigableMap<byte[], byte[]> map;
        private java.util.Map.Entry<byte[], byte[]> current;
        private java.util.Iterator<java.util.Map.Entry<byte[], byte[]>> it;

        SkipListMemTableIterator(ConcurrentNavigableMap<byte[], byte[]> map) {
            this.map = map;
            this.it = map.entrySet().iterator();
            this.current = null;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public void next() {
            current = it.next();
        }

        @Override
        public byte[] key() {
            if (current == null) throw new IllegalStateException("Call next() first");
            return current.getKey();
        }

        @Override
        public byte[] value() {
            if (current == null) throw new IllegalStateException("Call next() first");
            byte[] val = current.getValue();
            return val == TOMBSTONE ? null : val;
        }

        @Override
        public void seek(byte[] target) {
            it = map.tailMap(target, true).entrySet().iterator();
            current = null;
        }

        @Override
        public void seekToFirst() {
            it = map.entrySet().iterator();
            current = null;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
