package io.tinykv.storage.memtable;

import io.tinykv.storage.KVIterator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory KV store using prefix-sharded hash buckets of skip lists.
 *
 * Features:
 * - O(1) hash lookup to find the right bucket
 * - O(log n) within a bucket
 * - Good for: point lookups with common key prefixes
 *
 * Note: This is a simplified implementation.
 */
public class HashSkipListMemTable implements MemTable {

    private static final byte[] TOMBSTONE = new byte[0];
    private static final int DEFAULT_BUCKETS = 16;

    private final int bucketCount;
    private final Bucket[] buckets;
    private final AtomicLong approximateSize;

    private static class Bucket {
        final ConcurrentSkipListMap<byte[], byte[]> table;

        Bucket() {
            this.table = new ConcurrentSkipListMap<>(MemTable::compareBytes);
        }
    }

    public HashSkipListMemTable() {
        this(DEFAULT_BUCKETS);
    }

    public HashSkipListMemTable(int bucketCount) {
        this.bucketCount = bucketCount;
        this.buckets = new Bucket[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new Bucket();
        }
        this.approximateSize = new AtomicLong(0);
    }

    private int getBucket(byte[] key) {
        int h = 0;
        for (byte b : key) {
            h = 31 * h + Byte.toUnsignedInt(b);
        }
        return Math.abs(h % bucketCount);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        Bucket bucket = buckets[getBucket(key)];
        byte[] prev = bucket.table.put(key, value);
        if (prev == null) {
            approximateSize.addAndGet(key.length + value.length);
        } else {
            approximateSize.addAndGet(value.length - prev.length);
        }
    }

    @Override
    public void delete(byte[] key) {
        Bucket bucket = buckets[getBucket(key)];
        byte[] prev = bucket.table.put(key, TOMBSTONE);
        if (prev == null) {
            approximateSize.addAndGet(key.length);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        Bucket bucket = buckets[getBucket(key)];
        byte[] value = bucket.table.get(key);
        if (value == null || value == TOMBSTONE) {
            return null;
        }
        return value;
    }

    @Override
    public boolean contains(byte[] key) {
        return get(key) != null;
    }

    @Override
    public boolean isDeleted(byte[] key) {
        Bucket bucket = buckets[getBucket(key)];
        byte[] value = bucket.table.get(key);
        return value == TOMBSTONE;
    }

    @Override
    public long approximateSize() {
        return approximateSize.get();
    }

    @Override
    public boolean isEmpty() {
        for (Bucket bucket : buckets) {
            if (!bucket.table.isEmpty()) return false;
        }
        return true;
    }

    @Override
    public int entryCount() {
        int count = 0;
        for (Bucket bucket : buckets) {
            count += bucket.table.size();
        }
        return count;
    }

    @Override
    public KVIterator iterator() {
        return iterator(null, null);
    }

    @Override
    public KVIterator iterator(byte[] startKey, byte[] endKey) {
        return new HashSkipListIterator(buckets, startKey, endKey);
    }

    private static class HashSkipListIterator implements KVIterator {

        private final List<ConcurrentSkipListMap<byte[], byte[]>> maps;
        private final byte[] startKey;
        private final byte[] endKey;

        private int mapIndex;
        private Iterator<Map.Entry<byte[], byte[]>> currentIterator;
        private Map.Entry<byte[], byte[]> currentEntry;

        // Merged heap for global order
        private final PriorityQueue<HeapEntry> heap;

        private static class HeapEntry implements Comparable<HeapEntry> {
            final byte[] key;
            final byte[] value;
            final int sourceIndex;

            HeapEntry(byte[] key, byte[] value, int sourceIndex) {
                this.key = key;
                this.value = value;
                this.sourceIndex = sourceIndex;
            }

            @Override
            public int compareTo(HeapEntry o) {
                int cmp = MemTable.compareBytes(this.key, o.key);
                if (cmp != 0) return cmp;
                // Newer source first (just for consistency)
                return Integer.compare(this.sourceIndex, o.sourceIndex);
            }
        }

        HashSkipListIterator(Bucket[] buckets, byte[] startKey, byte[] endKey) {
            this.maps = new ArrayList<>();
            this.startKey = startKey;
            this.endKey = endKey;
            this.mapIndex = 0;

            // Collect all buckets
            for (Bucket b : buckets) {
                maps.add(b.table);
            }

            this.heap = new PriorityQueue<>();
            this.currentEntry = null;

            // Initialize heap with first entry from each bucket
            for (int i = 0; i < maps.size(); i++) {
                NavigableMap<byte[], byte[]> map;
                if (startKey != null) {
                    map = maps.get(i).tailMap(startKey, true);
                } else {
                    map = maps.get(i);
                }
                Iterator<Map.Entry<byte[], byte[]>> it = map.entrySet().iterator();
                if (it.hasNext()) {
                    Map.Entry<byte[], byte[]> e = it.next();
                    heap.add(new HeapEntry(e.getKey(), e.getValue(), i));
                }
            }
        }

        @Override
        public boolean hasNext() {
            while (!heap.isEmpty()) {
                HeapEntry top = heap.peek();
                // Check end key filter
                if (endKey != null && MemTable.compareBytes(top.key, endKey) >= 0) {
                    return false;
                }
                return true;
            }
            return false;
        }

        @Override
        public void next() {
            HeapEntry top = heap.poll();
            if (top != null) {
                currentEntry = new AbstractMap.SimpleEntry<>(top.key, top.value);
                // Refill the heap with next entry from this bucket
                NavigableMap<byte[], byte[]> map;
                if (startKey != null) {
                    map = maps.get(top.sourceIndex).tailMap(startKey, true);
                } else {
                    map = maps.get(top.sourceIndex);
                }
                Iterator<Map.Entry<byte[], byte[]>> it = map.tailMap(top.key, false).entrySet().iterator();
                if (it.hasNext()) {
                    Map.Entry<byte[], byte[]> e = it.next();
                    heap.add(new HeapEntry(e.getKey(), e.getValue(), top.sourceIndex));
                }
            } else {
                currentEntry = null;
            }
        }

        @Override
        public byte[] key() {
            if (currentEntry == null) throw new IllegalStateException("Call next() first");
            return currentEntry.getKey();
        }

        @Override
        public byte[] value() {
            if (currentEntry == null) throw new IllegalStateException("Call next() first");
            byte[] val = currentEntry.getValue();
            return val == TOMBSTONE ? null : val;
        }

        @Override
        public void seek(byte[] target) {
            // Reset and reinitialize
            heap.clear();
            currentEntry = null;
            for (int i = 0; i < maps.size(); i++) {
                NavigableMap<byte[], byte[]> map = maps.get(i).tailMap(target, true);
                Iterator<Map.Entry<byte[], byte[]>> it = map.entrySet().iterator();
                if (it.hasNext()) {
                    Map.Entry<byte[], byte[]> e = it.next();
                    heap.add(new HeapEntry(e.getKey(), e.getValue(), i));
                }
            }
        }

        @Override
        public void seekToFirst() {
            seek(null);
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
