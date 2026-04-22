package io.tinykv.storage.memtable;

import io.tinykv.common.Record.RecordType;
import io.tinykv.storage.KVIterator;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory sorted KV store backed by ConcurrentSkipListMap.
 * Thread-safe for concurrent reads and writes.
 */
public class MemTable {

    private static final byte[] TOMBSTONE = new byte[0];

    private final ConcurrentSkipListMap<byte[], byte[]> table;
    private long approximateSize;

    public MemTable() {
        this.table = new ConcurrentSkipListMap<>(MemTable::compareBytes);
        this.approximateSize = 0;
    }

    public void put(byte[] key, byte[] value) {
        byte[] prev = table.put(key, value);
        if (prev == null) {
            approximateSize += key.length + value.length;
        } else {
            approximateSize += value.length - prev.length;
        }
    }

    public void delete(byte[] key) {
        byte[] prev = table.put(key, TOMBSTONE);
        if (prev == null) {
            approximateSize += key.length;
        }
    }

    public byte[] get(byte[] key) {
        byte[] value = table.get(key);
        if (value == null || value == TOMBSTONE) {
            return null;
        }
        return value;
    }

    public boolean contains(byte[] key) {
        byte[] value = table.get(key);
        return value != null && value != TOMBSTONE;
    }

    public boolean isDeleted(byte[] key) {
        byte[] value = table.get(key);
        return value == TOMBSTONE;
    }

    public long approximateSize() {
        return approximateSize;
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    public int entryCount() {
        return table.size();
    }

    public KVIterator iterator() {
        return new MemTableIterator(table);
    }

    public KVIterator iterator(byte[] startKey, byte[] endKey) {
        ConcurrentNavigableMap<byte[], byte[]> subMap;
        if (startKey != null && endKey != null) {
            subMap = table.subMap(startKey, true, endKey, false);
        } else if (startKey != null) {
            subMap = table.tailMap(startKey, true);
        } else if (endKey != null) {
            subMap = table.headMap(endKey, false);
        } else {
            subMap = table;
        }
        return new MemTableIterator(subMap);
    }

    /**
     * Unsigned lexicographic byte array comparison.
     */
    public static int compareBytes(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.length - b.length;
    }

    /**
     * MemTable iterator wrapping the skip list map.
     */
    private static class MemTableIterator implements KVIterator {

        private final ConcurrentNavigableMap<byte[], byte[]> map;
        private java.util.Map.Entry<byte[], byte[]> current;
        private java.util.Iterator<java.util.Map.Entry<byte[], byte[]>> it;

        MemTableIterator(ConcurrentNavigableMap<byte[], byte[]> map) {
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
