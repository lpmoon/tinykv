package io.tinykv.storage.memtable;

import io.tinykv.storage.KVIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract test ensuring all MemTable implementations behave consistently.
 * Runs the same set of tests against each MemTable implementation.
 */
class MemTableContractTest {

    static Stream<MemTable> memTableProvider() {
        return Stream.of(
            new SkipListMemTable(),
            new VectorMemTable(),
            new HashSkipListMemTable()
        );
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testBasicPutGetDelete(MemTable table) {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        table.put(key, value);
        assertArrayEquals(value, table.get(key));

        table.delete(key);
        assertNull(table.get(key));
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testMultipleKeys(MemTable table) {
        for (int i = 0; i < 100; i++) {
            table.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        for (int i = 0; i < 100; i++) {
            assertArrayEquals(("value" + i).getBytes(), table.get(("key" + i).getBytes()));
        }
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testOverwriteSameKey(MemTable table) {
        byte[] key = "key1".getBytes();

        table.put(key, "v1".getBytes());
        assertArrayEquals("v1".getBytes(), table.get(key));

        table.put(key, "v2".getBytes());
        assertArrayEquals("v2".getBytes(), table.get(key));

        table.put(key, "v3".getBytes());
        assertArrayEquals("v3".getBytes(), table.get(key));
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testContains(MemTable table) {
        byte[] key = "test-key".getBytes();

        assertFalse(table.contains(key));

        table.put(key, "value".getBytes());
        assertTrue(table.contains(key));

        table.delete(key);
        assertFalse(table.contains(key));
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testIsDeleted(MemTable table) {
        byte[] key = "test-key".getBytes();

        assertFalse(table.isDeleted(key));

        table.put(key, "value".getBytes());
        assertFalse(table.isDeleted(key));

        table.delete(key);
        assertTrue(table.isDeleted(key));
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testIsEmpty(MemTable table) {
        assertTrue(table.isEmpty());

        table.put("key1".getBytes(), "value1".getBytes());
        assertFalse(table.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testEntryCount(MemTable table) {
        assertEquals(0, table.entryCount());

        table.put("key1".getBytes(), "v1".getBytes());
        assertEquals(1, table.entryCount());

        table.put("key2".getBytes(), "v2".getBytes());
        table.put("key3".getBytes(), "v3".getBytes());
        assertEquals(3, table.entryCount());

        table.put("key2".getBytes(), "v2-updated".getBytes());
        assertEquals(3, table.entryCount());

        table.delete("key4".getBytes());
        assertEquals(4, table.entryCount());
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testApproximateSize(MemTable table) {
        long initialSize = table.approximateSize();

        table.put("key".getBytes(), "value".getBytes());
        assertTrue(table.approximateSize() > initialSize);
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testFullIterator(MemTable table) throws Exception {
        String[] expectedKeys = {"apple", "banana", "cherry", "date", "elderberry"};

        for (String key : expectedKeys) {
            table.put(key.getBytes(), key.toUpperCase().getBytes());
        }

        List<String> actualKeys = new ArrayList<>();
        KVIterator it = table.iterator();
        try {
            while (it.hasNext()) {
                it.next();
                actualKeys.add(new String(it.key()));
            }
        } finally {
            it.close();
        }

        assertEquals(Arrays.asList(expectedKeys), actualKeys);
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testRangeIterator(MemTable table) throws Exception {
        for (int i = 0; i < 10; i++) {
            table.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        List<String> actualKeys = new ArrayList<>();
        KVIterator it = table.iterator("key2".getBytes(), "key7".getBytes());
        try {
            while (it.hasNext()) {
                it.next();
                actualKeys.add(new String(it.key()));
            }
        } finally {
            it.close();
        }

        assertEquals(Arrays.asList("key2", "key3", "key4", "key5", "key6"), actualKeys);
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testIteratorValueWithTombstone(MemTable table) throws Exception {
        table.put("key1".getBytes(), "value1".getBytes());
        table.delete("key1".getBytes());

        KVIterator it = table.iterator();
        try {
            assertTrue(it.hasNext());
            it.next();
            assertNull(it.value());
        } finally {
            it.close();
        }
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testIteratorSeek(MemTable table) throws Exception {
        for (int i = 0; i < 10; i++) {
            table.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        KVIterator it = table.iterator();
        try {
            it.seek("key5".getBytes());
            assertTrue(it.hasNext());
            it.next();
            assertEquals("key5", new String(it.key()));
        } finally {
            it.close();
        }
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testIteratorSeekToFirst(MemTable table) throws Exception {
        for (int i = 5; i < 10; i++) {
            table.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        KVIterator it = table.iterator();
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

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testDeleteNonExistentKey(MemTable table) {
        table.delete("nonexistent".getBytes());
        assertTrue(table.isDeleted("nonexistent".getBytes()));
        assertEquals(1, table.entryCount());
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testLastWriteWins(MemTable table) {
        byte[] key = "test-key".getBytes();

        table.put(key, "first".getBytes());
        table.put(key, "second".getBytes());
        table.put(key, "third".getBytes());

        assertArrayEquals("third".getBytes(), table.get(key));
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testInterleavedOperations(MemTable table) {
        table.put("a".getBytes(), "1".getBytes());
        assertEquals("1", new String(table.get("a".getBytes())));

        table.put("b".getBytes(), "2".getBytes());
        assertEquals("2", new String(table.get("b".getBytes())));

        table.delete("a".getBytes());
        assertNull(table.get("a".getBytes()));

        table.put("a".getBytes(), "3".getBytes());
        assertEquals("3", new String(table.get("a".getBytes())));

        table.put("c".getBytes(), "4".getBytes());
        assertEquals("4", new String(table.get("c".getBytes())));
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testByteKeys(MemTable table) {
        byte[] key1 = new byte[]{0x01, 0x02};
        byte[] key2 = new byte[]{0x01, 0x03};
        byte[] key3 = new byte[]{0x02};

        table.put(key1, "value1".getBytes());
        table.put(key2, "value2".getBytes());
        table.put(key3, "value3".getBytes());

        assertArrayEquals("value1".getBytes(), table.get(key1));
        assertArrayEquals("value2".getBytes(), table.get(key2));
        assertArrayEquals("value3".getBytes(), table.get(key3));
    }

    @ParameterizedTest
    @MethodSource("memTableProvider")
    void testIteratorThrowsOnInvalidAccess(MemTable table) throws Exception {
        table.put("key1".getBytes(), "value1".getBytes());

        KVIterator it = table.iterator();
        try {
            assertThrows(IllegalStateException.class, it::key);
            assertThrows(IllegalStateException.class, it::value);
        } finally {
            it.close();
        }
    }
}
