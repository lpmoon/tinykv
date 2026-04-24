package io.tinykv;

import io.tinykv.common.Config;
import io.tinykv.storage.LSMTree;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for single-node TinyKV storage engine.
 */
public class SingleNodeIntegrationTest extends IntegrationTestBase {

    static class KV {
        final byte[] key;
        final byte[] value;
        KV(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    @Test
    void testBasicPutGetDelete() throws Exception {
        Config config = createConfig("node1");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();

            byte[] key = "hello".getBytes();
            byte[] value = "world".getBytes();
            lsmTree.put(key, value);

            Optional<byte[]> got = lsmTree.get(key);
            assertTrue(got.isPresent());
            assertArrayEquals(value, got.get());

            lsmTree.delete(key);
            Optional<byte[]> afterDelete = lsmTree.get(key);
            assertTrue(afterDelete.isEmpty());
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testOverwrite() throws Exception {
        Config config = createConfig("node1");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();

            byte[] key = "counter".getBytes();

            lsmTree.put(key, "v1".getBytes());
            assertArrayEquals("v1".getBytes(), lsmTree.get(key).orElseThrow());

            lsmTree.put(key, "v2".getBytes());
            assertArrayEquals("v2".getBytes(), lsmTree.get(key).orElseThrow());

            lsmTree.put(key, "v3".getBytes());
            assertArrayEquals("v3".getBytes(), lsmTree.get(key).orElseThrow());
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testGetNonExistent() throws Exception {
        Config config = createConfig("node1");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();

            Optional<byte[]> result = lsmTree.get("nonexistent".getBytes());
            assertTrue(result.isEmpty());
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testMultipleKeys() throws Exception {
        Config config = createConfig("node1");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();

            for (int i = 0; i < 20; i++) {
                String key = "key-" + i;
                String value = "value-" + i;
                lsmTree.put(key.getBytes(), value.getBytes());
            }

            for (int i = 0; i < 20; i++) {
                String key = "key-" + i;
                String value = "value-" + i;
                Optional<byte[]> got = lsmTree.get(key.getBytes());
                assertTrue(got.isPresent());
                assertArrayEquals(value.getBytes(), got.get());
            }
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testScan() throws Exception {
        Config config = createConfig("node1");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();

            for (int i = 0; i < 20; i++) {
                String key = String.format("key-%03d", i);
                String value = "value-" + i;
                lsmTree.put(key.getBytes(), value.getBytes());
            }

            List<KV> results = new ArrayList<>();
            try (var iter = lsmTree.scan("key-005".getBytes(), "key-015".getBytes())) {
                while (iter.hasNext()) {
                    iter.next();
                    results.add(new KV(iter.key(), iter.value()));
                }
            }

            assertEquals(10, results.size());
            for (int i = 0; i < 10; i++) {
                String expectedKey = String.format("key-%03d", i + 5);
                String expectedValue = "value-" + (i + 5);
                assertEquals(expectedKey, new String(results.get(i).key));
                assertEquals(expectedValue, new String(results.get(i).value));
            }
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testLargeValues() throws Exception {
        Config config = createConfig("node1");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();

            byte[] largeValue = new byte[10 * 1024];
            for (int i = 0; i < largeValue.length; i++) {
                largeValue[i] = (byte) (i % 256);
            }
            byte[] key = "large".getBytes();

            lsmTree.put(key, largeValue);
            Optional<byte[]> got = lsmTree.get(key);
            assertTrue(got.isPresent());
            assertArrayEquals(largeValue, got.get());
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testMixedOperations() throws Exception {
        Config config = createConfig("node1");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();

            for (int i = 0; i < 20; i++) {
                lsmTree.put(("key-" + i).getBytes(), ("value-" + i).getBytes());
            }

            for (int i = 0; i < 20; i += 2) {
                lsmTree.delete(("key-" + i).getBytes());
            }

            for (int i = 1; i < 20; i += 4) {
                lsmTree.put(("key-" + i).getBytes(), ("new-value-" + i).getBytes());
            }

            for (int i = 0; i < 20; i++) {
                Optional<byte[]> got = lsmTree.get(("key-" + i).getBytes());
                if (i % 2 == 0) {
                    assertTrue(got.isEmpty());
                } else if (i % 4 == 1) {
                    assertTrue(got.isPresent());
                    assertEquals("new-value-" + i, new String(got.get()));
                } else {
                    assertTrue(got.isPresent());
                    assertEquals("value-" + i, new String(got.get()));
                }
            }
        } finally {
            lsmTree.close();
        }
    }
}
