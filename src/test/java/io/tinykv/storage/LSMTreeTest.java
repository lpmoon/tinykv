package io.tinykv.storage;

import io.tinykv.common.Config;
import io.tinykv.storage.memtable.MemTableType;
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

    // ========================================================================
    // 多 MemTable 架构相关测试
    // ========================================================================

    @Test
    void testMultipleImmutableMemTables() throws IOException, InterruptedException {
        // 使用小的memtable size，触发多次切换
        // 把maxWriteBufferNumber设大一点，避免触发flush到SSTable
        Config smallConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(200)
                .setMaxWriteBufferNumber(100);

        // 先关闭默认的
        lsmTree.close();

        lsmTree = new LSMTree(smallConfig);
        lsmTree.recover();

        // 写入足够多的数据，应该触发多次memtable切换
        for (int i = 0; i < 10; i++) {
            String key = String.format("key%03d", i);
            String value = String.format("value%03d", i);
            lsmTree.put(key.getBytes(), value.getBytes());
        }

        // 验证读取正确（从活跃或任意immutable memtable都应该能读到）
        for (int i = 0; i < 10; i++) {
            String key = String.format("key%03d", i);
            String value = String.format("value%03d", i);
            Optional<byte[]> result = lsmTree.get(key.getBytes());
            assertTrue(result.isPresent(), "Key " + key + " should exist");
            assertArrayEquals(value.getBytes(), result.get());
        }
    }

    @Test
    void testReadFromImmutableMemTable() throws IOException, InterruptedException {
        Config smallConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(200)
                .setMaxWriteBufferNumber(100);

        lsmTree.close();
        lsmTree = new LSMTree(smallConfig);
        lsmTree.recover();

        // 第一轮写入
        lsmTree.put("key1".getBytes(), "value1".getBytes());
        lsmTree.put("key2".getBytes(), "value2".getBytes());

        // 写入更多数据触发memtable切换
        for (int i = 0; i < 5; i++) {
            String key = String.format("fill%03d", i);
            lsmTree.put(key.getBytes(), "fillvalue".getBytes());
        }

        // 此时前两个key应该在immutable memtable中了
        // 但仍然能读到
        assertArrayEquals("value1".getBytes(), lsmTree.get("key1".getBytes()).orElse(null));
        assertArrayEquals("value2".getBytes(), lsmTree.get("key2".getBytes()).orElse(null));

        // 在新的活跃memtable中覆盖key1
        lsmTree.put("key1".getBytes(), "newvalue1".getBytes());

        // 应该读到新值
        assertArrayEquals("newvalue1".getBytes(), lsmTree.get("key1".getBytes()).orElse(null));
    }

    @Test
    void testScanAcrossMultipleMemTables() throws IOException, InterruptedException {
        Config smallConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(200)
                .setMaxWriteBufferNumber(100);

        lsmTree.close();
        lsmTree = new LSMTree(smallConfig);
        lsmTree.recover();

        // 写入多批数据，会分布在不同的memtable中
        String[] keys = new String[10];
        for (int i = 0; i < 10; i++) {
            keys[i] = String.format("key%03d", i);
            lsmTree.put(keys[i].getBytes(), ("value" + i).getBytes());

            // 每2个key填充一些数据来触发memtable切换
            if (i % 2 == 1) {
                for (int j = 0; j < 3; j++) {
                    lsmTree.put(("fill" + i + "-" + j).getBytes(), "fill".getBytes());
                }
            }
        }

        // 范围扫描
        int count = 0;
        try (KVIterator it = lsmTree.scan("key003".getBytes(), "key007".getBytes())) {
            while (it.hasNext()) {
                it.next();
                String key = new String(it.key());
                assertTrue(key.startsWith("key"));
                count++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // key003, key004, key005, key006
        assertEquals(4, count);
    }

    @Test
    void testWriteStall() throws IOException, InterruptedException {
        // 配置小的maxWriteBufferNumber
        Config stallConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(200)
                .setMaxWriteBufferNumber(3);

        lsmTree.close();
        lsmTree = new LSMTree(stallConfig);
        lsmTree.recover();

        // 写入数据
        for (int i = 0; i < 10; i++) {
            String key = String.format("key%03d", i);
            lsmTree.put(key.getBytes(), ("value" + i).getBytes());
        }

        // 验证所有数据都写入成功
        for (int i = 0; i < 10; i++) {
            String key = String.format("key%03d", i);
            assertTrue(lsmTree.get(key.getBytes()).isPresent(), "Key " + key + " should exist");
        }
    }

    @Test
    void testFlushAllMemTablesOnClose() throws IOException, InterruptedException {
        Config smallConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(200)
                .setMaxWriteBufferNumber(4);

        lsmTree.close();
        lsmTree = new LSMTree(smallConfig);
        lsmTree.recover();

        // 写入数据
        for (int i = 0; i < 5; i++) {
            lsmTree.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        // 关闭
        lsmTree.close();
        lsmTree = null;

        // 重新打开并恢复
        lsmTree = new LSMTree(config);
        lsmTree.recover();

        // 验证数据
        for (int i = 0; i < 5; i++) {
            Optional<byte[]> result = lsmTree.get(("key" + i).getBytes());
            assertTrue(result.isPresent(), "Key " + i + " should exist after recovery");
            assertArrayEquals(("value" + i).getBytes(), result.get());
        }
    }

    @Test
    void testDeletionInImmutableMemTable() throws IOException, InterruptedException {
        Config smallConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(200)
                .setMaxWriteBufferNumber(100);

        lsmTree.close();
        lsmTree = new LSMTree(smallConfig);
        lsmTree.recover();

        // 写入一个key
        lsmTree.put("key1".getBytes(), "value1".getBytes());

        // 触发memtable切换
        for (int i = 0; i < 5; i++) {
            lsmTree.put(("fill" + i).getBytes(), "fill".getBytes());
        }

        // 现在删除key1（此时key1在immutable中）
        lsmTree.delete("key1".getBytes());

        // 应该读不到了
        assertTrue(lsmTree.get("key1".getBytes()).isEmpty());

        // 再次写回key1
        lsmTree.put("key1".getBytes(), "value1-new".getBytes());
        assertArrayEquals("value1-new".getBytes(), lsmTree.get("key1".getBytes()).orElse(null));
    }

    // =========================================================================
    // Tests for different MemTable types
    // =========================================================================

    @Test
    void testVectorMemTable() throws IOException, InterruptedException {
        Config vectorConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(1024)
                .setMaxWriteBufferNumber(4)
                .setMemTableType(MemTableType.VECTOR);

        lsmTree.close();
        lsmTree = new LSMTree(vectorConfig);
        lsmTree.recover();

        // Write data
        for (int i = 0; i < 10; i++) {
            lsmTree.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        // Delete one
        lsmTree.delete("key5".getBytes());

        // Verify reads
        for (int i = 0; i < 10; i++) {
            Optional<byte[]> result = lsmTree.get(("key" + i).getBytes());
            if (i == 5) {
                assertTrue(result.isEmpty());
            } else {
                assertTrue(result.isPresent());
                assertArrayEquals(("value" + i).getBytes(), result.get());
            }
        }

        // Verify range scan
        int count = 0;
        try (KVIterator it = lsmTree.scan("key2".getBytes(), "key8".getBytes())) {
            while (it.hasNext()) {
                it.next();
                String key = new String(it.key());
                assertFalse(key.equals("key5"));
                count++;
            }
        }
        assertEquals(5, count);
    }

    @Test
    void testHashSkipListMemTable() throws IOException, InterruptedException {
        Config hashConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(1024)
                .setMaxWriteBufferNumber(4)
                .setMemTableType(MemTableType.HASH_SKIP_LIST)
                .setHashBuckets(8);

        lsmTree.close();
        lsmTree = new LSMTree(hashConfig);
        lsmTree.recover();

        // Write data
        for (int i = 0; i < 20; i++) {
            lsmTree.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }

        // Overwrite some
        for (int i = 0; i < 5; i++) {
            lsmTree.put(("key" + i).getBytes(), ("new_value" + i).getBytes());
        }

        // Verify reads
        for (int i = 0; i < 20; i++) {
            Optional<byte[]> result = lsmTree.get(("key" + i).getBytes());
            assertTrue(result.isPresent());
            String expected = i < 5 ? ("new_value" + i) : ("value" + i);
            assertArrayEquals(expected.getBytes(), result.get());
        }
    }

    @Test
    void testSkipListMemTable() throws IOException, InterruptedException {
        Config skipListConfig = new Config()
                .setDataDir(TEST_DIR)
                .setMemTableSize(1024)
                .setMaxWriteBufferNumber(4)
                .setMemTableType(MemTableType.SKIP_LIST);

        lsmTree.close();
        lsmTree = new LSMTree(skipListConfig);
        lsmTree.recover();

        // Write and verify
        for (int i = 0; i < 15; i++) {
            lsmTree.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }
        for (int i = 0; i < 15; i++) {
            Optional<byte[]> result = lsmTree.get(("key" + i).getBytes());
            assertTrue(result.isPresent());
            assertArrayEquals(("value" + i).getBytes(), result.get());
        }
    }
}
