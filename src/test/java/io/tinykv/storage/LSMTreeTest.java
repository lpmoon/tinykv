package io.tinykv.storage;

import io.tinykv.common.Config;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LSMTreeTest {

    private static final String TEST_DIR = "/tmp/tinykv-test-lsm";
    private LSMTree lsmTree;
    private Config config;

    @BeforeEach
    void setUp() throws IOException {
        cleanup();
        config = new Config().setDataDir(TEST_DIR).setMemTableSize(1024); // Small for testing
        lsmTree = new LSMTree(config);
        lsmTree.recover();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (lsmTree != null) {
            lsmTree.close();
        }
        cleanup();
    }

    private void cleanup() throws IOException {
        Path dir = Paths.get(TEST_DIR);
        if (Files.exists(dir)) {
            try (var stream = Files.walk(dir)) {
                stream.sorted(Comparator.reverseOrder())
                      .map(Path::toFile)
                      .forEach(File::delete);
            }
        }
    }

    @Test
    void testPutAndGet() {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        lsmTree.put(key, value);

        Optional<byte[]> result = lsmTree.get(key);
        assertTrue(result.isPresent());
        assertArrayEquals(value, result.get());
    }

    @Test
    void testGetNonExistent() {
        Optional<byte[]> result = lsmTree.get("nonexistent".getBytes());
        assertTrue(result.isEmpty());
    }

    @Test
    void testDelete() {
        byte[] key = "key1".getBytes();
        byte[] value = "value1".getBytes();

        lsmTree.put(key, value);
        assertTrue(lsmTree.get(key).isPresent());

        lsmTree.delete(key);
        assertTrue(lsmTree.get(key).isEmpty());
    }

    @Test
    void testOverwrite() {
        byte[] key = "key1".getBytes();
        lsmTree.put(key, "value1".getBytes());
        lsmTree.put(key, "value2".getBytes());

        Optional<byte[]> result = lsmTree.get(key);
        assertTrue(result.isPresent());
        assertArrayEquals("value2".getBytes(), result.get());
    }

    @Test
    void testBatchWrite() {
        Batch batch = new Batch()
                .put("k1".getBytes(), "v1".getBytes())
                .put("k2".getBytes(), "v2".getBytes())
                .put("k3".getBytes(), "v3".getBytes())
                .delete("k1".getBytes());

        lsmTree.write(batch);

        assertTrue(lsmTree.get("k1".getBytes()).isEmpty());
        assertArrayEquals("v2".getBytes(), lsmTree.get("k2".getBytes()).orElse(null));
        assertArrayEquals("v3".getBytes(), lsmTree.get("k3".getBytes()).orElse(null));
    }

    @Test
    void testRangeScan() {
        for (int i = 0; i < 10; i++) {
            String key = String.format("key%03d", i);
            lsmTree.put(key.getBytes(), ("value" + i).getBytes());
        }

        int count = 0;
        try (KVIterator it = lsmTree.scan("key003".getBytes(), "key007".getBytes())) {
            while (it.hasNext()) {
                it.next();
                count++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertEquals(4, count); // key003, key004, key005, key006
    }

    @Test
    void testRecovery() throws IOException {
        lsmTree.put("key1".getBytes(), "value1".getBytes());
        lsmTree.put("key2".getBytes(), "value2".getBytes());
        lsmTree.close();

        // Reopen and recover
        lsmTree = new LSMTree(config);
        lsmTree.recover();

        assertArrayEquals("value1".getBytes(), lsmTree.get("key1".getBytes()).orElse(null));
        assertArrayEquals("value2".getBytes(), lsmTree.get("key2".getBytes()).orElse(null));
    }
}
