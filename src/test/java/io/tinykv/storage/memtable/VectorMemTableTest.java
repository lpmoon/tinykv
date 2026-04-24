package io.tinykv.storage.memtable;

import io.tinykv.storage.KVIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for VectorMemTable.
 */
class VectorMemTableTest {

    private VectorMemTable memTable;

    @BeforeEach
    void setUp() {
        memTable = new VectorMemTable();
    }

    @Test
    void testPutAndGet() {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        memTable.put(key, value);

        byte[] result = memTable.get(key);
        assertNotNull(result);
        assertArrayEquals(value, result);
    }

    @Test
    void testGetNonExistent() {
        byte[] result = memTable.get("nonexistent".getBytes());
        assertNull(result);
    }

    @Test
    void testContains() {
        byte[] key = "key1".getBytes();

        assertFalse(memTable.contains(key));

        memTable.put(key, "value1".getBytes());

        assertTrue(memTable.contains(key));
    }

    @Test
    void testDelete() {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        memTable.put(key, value);
        assertNotNull(memTable.get(key));
        assertTrue(memTable.contains(key));

        memTable.delete(key);

        assertNull(memTable.get(key));
        assertFalse(memTable.contains(key));
        assertTrue(memTable.isDeleted(key));
    }

    @Test
    void testOverwrite() {
        byte[] key = "key1".getBytes();

        memTable.put(key, "value1".getBytes());
        assertArrayEquals("value1".getBytes(), memTable.get(key));

        memTable.put(key, "value2".getBytes());
        assertArrayEquals("value2".getBytes(), memTable.get(key));
    }

    @Test
    void testMultiplePutsAndDeletes() {
        for (int i = 0; i < 100; i++) {
            memTable.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        for (int i = 0; i < 50; i++) {
            memTable.delete(("key" + i).getBytes());
        }

        for (int i = 0; i < 100; i++) {
            if (i < 50) {
                assertNull(memTable.get(("key" + i).getBytes()));
                assertTrue(memTable.isDeleted(("key" + i).getBytes()));
            } else {
                assertNotNull(memTable.get(("key" + i).getBytes()));
                assertArrayEquals(("value" + i).getBytes(), memTable.get(("key" + i).getBytes()));
            }
        }
    }

    @Test
    void testIsEmpty() {
        assertTrue(memTable.isEmpty());

        memTable.put("key1".getBytes(), "value1".getBytes());
        assertFalse(memTable.isEmpty());

        memTable.delete("key1".getBytes());
        assertFalse(memTable.isEmpty()); // Tombstone still present
    }

    @Test
    void testEntryCount() {
        assertEquals(0, memTable.entryCount());

        memTable.put("key1".getBytes(), "value1".getBytes());
        assertEquals(1, memTable.entryCount());

        memTable.put("key2".getBytes(), "value2".getBytes());
        assertEquals(2, memTable.entryCount());

        memTable.put("key1".getBytes(), "value1-updated".getBytes());
        assertEquals(2, memTable.entryCount()); // Overwrite doesn't increase count

        memTable.delete("key3".getBytes());
        assertEquals(3, memTable.entryCount()); // Delete adds tombstone
    }

    @Test
    void testApproximateSize() {
        assertEquals(0, memTable.approximateSize());

        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();
        memTable.put(key, value);

        long expectedSize = key.length + value.length;
        assertEquals(expectedSize, memTable.approximateSize());

        memTable.put("key2".getBytes(), "value2".getBytes());
        assertTrue(memTable.approximateSize() > expectedSize);
    }

    @Test
    void testApproximateSizeWithOverwrite() {
        byte[] key = "key1".getBytes();

        memTable.put(key, "short".getBytes());
        long size1 = memTable.approximateSize();

        memTable.put(key, "much-longer-value".getBytes());
        long size2 = memTable.approximateSize();

        assertTrue(size2 > size1);
    }

    @Test
    void testIteratorFullScan() throws Exception {
        Map<String, String> data = new TreeMap<>();
        for (int i = 0; i < 20; i++) {
            String key = String.format("key%03d", i);
            String value = "value" + i;
            data.put(key, value);
            memTable.put(key.getBytes(), value.getBytes());
        }

        List<String> keysSeen = new ArrayList<>();
        KVIterator it = memTable.iterator();
        try {
            while (it.hasNext()) {
                it.next();
                keysSeen.add(new String(it.key()));
            }
        } finally {
            it.close();
        }

        assertEquals(20, keysSeen.size());

        List<String> expectedKeys = new ArrayList<>(data.keySet());
        assertEquals(expectedKeys, keysSeen);
    }

    @Test
    void testIteratorRange() throws Exception {
        for (int i = 0; i < 10; i++) {
            memTable.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        List<String> keysSeen = new ArrayList<>();
        KVIterator it = memTable.iterator("key3".getBytes(), "key7".getBytes());
        try {
            while (it.hasNext()) {
                it.next();
                keysSeen.add(new String(it.key()));
            }
        } finally {
            it.close();
        }

        assertEquals(4, keysSeen.size());
        assertEquals("key3", keysSeen.get(0));
        assertEquals("key4", keysSeen.get(1));
        assertEquals("key5", keysSeen.get(2));
        assertEquals("key6", keysSeen.get(3));
    }

    @Test
    void testIteratorStartOnly() throws Exception {
        for (int i = 0; i < 10; i++) {
            memTable.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        List<String> keysSeen = new ArrayList<>();
        KVIterator it = memTable.iterator("key5".getBytes(), null);
        try {
            while (it.hasNext()) {
                it.next();
                keysSeen.add(new String(it.key()));
            }
        } finally {
            it.close();
        }

        assertEquals(5, keysSeen.size());
        assertEquals("key5", keysSeen.get(0));
        assertEquals("key9", keysSeen.get(4));
    }

    @Test
    void testIteratorEndOnly() throws Exception {
        for (int i = 0; i < 10; i++) {
            memTable.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        List<String> keysSeen = new ArrayList<>();
        KVIterator it = memTable.iterator(null, "key5".getBytes());
        try {
            while (it.hasNext()) {
                it.next();
                keysSeen.add(new String(it.key()));
            }
        } finally {
            it.close();
        }

        assertEquals(5, keysSeen.size());
        assertEquals("key0", keysSeen.get(0));
        assertEquals("key4", keysSeen.get(4));
    }

    @Test
    void testIteratorWithTombstones() throws Exception {
        memTable.put("key1".getBytes(), "value1".getBytes());
        memTable.put("key2".getBytes(), "value2".getBytes());
        memTable.delete("key2".getBytes());
        memTable.put("key3".getBytes(), "value3".getBytes());

        List<String> keysSeen = new ArrayList<>();
        List<String> valuesSeen = new ArrayList<>();

        KVIterator it = memTable.iterator();
        try {
            while (it.hasNext()) {
                it.next();
                keysSeen.add(new String(it.key()));
                valuesSeen.add(it.value() == null ? "null" : new String(it.value()));
            }
        } finally {
            it.close();
        }

        assertEquals(3, keysSeen.size());
        assertEquals("key1", keysSeen.get(0));
        assertEquals("key2", keysSeen.get(1));
        assertEquals("key3", keysSeen.get(2));
        assertEquals("value1", valuesSeen.get(0));
        assertEquals("null", valuesSeen.get(1));
        assertEquals("value3", valuesSeen.get(2));
    }

    @Test
    void testSeek() throws Exception {
        for (int i = 0; i < 10; i++) {
            memTable.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        KVIterator it = memTable.iterator();
        try {
            it.seek("key3".getBytes());

            assertTrue(it.hasNext());
            it.next();
            assertEquals("key3", new String(it.key()));
            assertArrayEquals("value3".getBytes(), it.value());
        } finally {
            it.close();
        }
    }

    @Test
    void testSeekToFirst() throws Exception {
        for (int i = 5; i < 10; i++) {
            memTable.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        KVIterator it = memTable.iterator();
        try {
            it.seek("key7".getBytes());
            it.next();
            assertEquals("key7", new String(it.key()));

            it.seekToFirst();
            assertTrue(it.hasNext());
            it.next();
            assertEquals("key5", new String(it.key()));
        } finally {
            it.close();
        }
    }

    @Test
    void testLexicographicOrdering() throws Exception {
        String[] keys = {
            "apple", "banana", "cherry", "date", "elderberry", "fig", "grape"
        };

        Random r = new Random(42);
        List<String> shuffledKeys = new ArrayList<>(Arrays.asList(keys));
        Collections.shuffle(shuffledKeys, r);

        for (String key : shuffledKeys) {
            memTable.put(key.getBytes(), key.toUpperCase().getBytes());
        }

        List<String> seenKeys = new ArrayList<>();
        KVIterator it = memTable.iterator();
        try {
            while (it.hasNext()) {
                it.next();
                seenKeys.add(new String(it.key()));
            }
        } finally {
            it.close();
        }

        assertEquals(Arrays.asList(keys), seenKeys);
    }

    @Test
    void testLastWriteWins() {
        byte[] key = "key1".getBytes();

        memTable.put(key, "first".getBytes());
        memTable.put(key, "second".getBytes());
        memTable.put(key, "third".getBytes());

        assertArrayEquals("third".getBytes(), memTable.get(key));
    }

    @Test
    void testInterleavedWriteAndRead() {
        memTable.put("key1".getBytes(), "value1".getBytes());
        assertArrayEquals("value1".getBytes(), memTable.get("key1".getBytes()));

        memTable.put("key2".getBytes(), "value2".getBytes());
        assertArrayEquals("value2".getBytes(), memTable.get("key2".getBytes()));

        memTable.put("key1".getBytes(), "updated".getBytes());
        assertArrayEquals("updated".getBytes(), memTable.get("key1".getBytes()));
    }

    @Test
    void testEmptyKey() {
        byte[] emptyKey = new byte[0];
        memTable.put(emptyKey, "empty-value".getBytes());

        assertNotNull(memTable.get(emptyKey));
        assertArrayEquals("empty-value".getBytes(), memTable.get(emptyKey));
    }

    @Test
    void testLargeValue() {
        byte[] largeValue = new byte[10000];
        Arrays.fill(largeValue, (byte) 0xAA);
        byte[] key = "large-key".getBytes();

        memTable.put(key, largeValue);

        byte[] result = memTable.get(key);
        assertNotNull(result);
        assertArrayEquals(largeValue, result);
    }

    @Test
    void testIteratorKeyThrowsWithoutNext() throws Exception {
        KVIterator it = memTable.iterator();
        try {
            assertThrows(IllegalStateException.class, it::key);
        } finally {
            it.close();
        }
    }

    @Test
    void testIteratorValueThrowsWithoutNext() throws Exception {
        KVIterator it = memTable.iterator();
        try {
            assertThrows(IllegalStateException.class, it::value);
        } finally {
            it.close();
        }
    }
}
