package io.tinykv;

import io.tinykv.common.Config;
import io.tinykv.replication.common.WriteOp;
import io.tinykv.replication.sync.SyncReplicator;
import io.tinykv.storage.LSMTree;
import io.tinykv.storage.MVCCStorage;
import io.tinykv.transaction.TimestampOracle;
import io.tinykv.transaction.Txn;
import io.tinykv.transaction.TxnManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for transactional operations.
 */
public class TransactionIntegrationTest extends IntegrationTestBase {

    static class MockReplicator extends SyncReplicator {
        private final MVCCStorage mvccStorage;

        public MockReplicator(MVCCStorage mvccStorage) {
            super(null, null, null);
            this.mvccStorage = mvccStorage;
        }

        @Override
        public java.util.concurrent.CompletableFuture<io.tinykv.replication.common.ReplicateResult> batchPut(List<WriteOp> ops) {
            mvccStorage.writeTxnBatch(ops);
            return java.util.concurrent.CompletableFuture.completedFuture(
                new io.tinykv.replication.common.ReplicateResult(0, true)
            );
        }
    }

    @Test
    void testBasicTransaction() throws Exception {
        Config config = createConfig("txn-node");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();
            TimestampOracle tsOracle = new TimestampOracle();
            MVCCStorage mvccStorage = new MVCCStorage(lsmTree, tsOracle);
            MockReplicator replicator = new MockReplicator(mvccStorage);
            TxnManager txnManager = new TxnManager(mvccStorage, replicator, tsOracle);

            txnManager.execute(txn -> {
                txn.put("key1".getBytes(), "value1".getBytes());
                txn.put("key2".getBytes(), "value2".getBytes());
            });

            Optional<byte[]> v1 = mvccStorage.get("key1".getBytes());
            Optional<byte[]> v2 = mvccStorage.get("key2".getBytes());
            assertTrue(v1.isPresent());
            assertTrue(v2.isPresent());
            assertArrayEquals("value1".getBytes(), v1.get());
            assertArrayEquals("value2".getBytes(), v2.get());
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testTransactionReadYourWrites() throws Exception {
        Config config = createConfig("txn-node");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();
            TimestampOracle tsOracle = new TimestampOracle();
            MVCCStorage mvccStorage = new MVCCStorage(lsmTree, tsOracle);
            MockReplicator replicator = new MockReplicator(mvccStorage);
            TxnManager txnManager = new TxnManager(mvccStorage, replicator, tsOracle);

            txnManager.execute(txn -> {
                txn.put("key".getBytes(), "original".getBytes());
                Optional<byte[]> read1 = txn.get("key".getBytes());
                assertTrue(read1.isPresent());
                assertArrayEquals("original".getBytes(), read1.get());

                txn.put("key".getBytes(), "updated".getBytes());
                Optional<byte[]> read2 = txn.get("key".getBytes());
                assertTrue(read2.isPresent());
                assertArrayEquals("updated".getBytes(), read2.get());
            });

            Optional<byte[]> finalValue = mvccStorage.get("key".getBytes());
            assertTrue(finalValue.isPresent());
            assertArrayEquals("updated".getBytes(), finalValue.get());
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testTransactionDelete() throws Exception {
        Config config = createConfig("txn-node");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();
            TimestampOracle tsOracle = new TimestampOracle();
            MVCCStorage mvccStorage = new MVCCStorage(lsmTree, tsOracle);
            MockReplicator replicator = new MockReplicator(mvccStorage);
            TxnManager txnManager = new TxnManager(mvccStorage, replicator, tsOracle);

            txnManager.execute(txn -> {
                txn.put("keep".getBytes(), "v1".getBytes());
                txn.put("delete".getBytes(), "v2".getBytes());
            });

            txnManager.execute(txn -> {
                txn.delete("delete".getBytes());
            });

            assertTrue(mvccStorage.get("keep".getBytes()).isPresent());
            assertTrue(mvccStorage.get("delete".getBytes()).isEmpty());
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testMultipleTransactions() throws Exception {
        Config config = createConfig("txn-node");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();
            TimestampOracle tsOracle = new TimestampOracle();
            MVCCStorage mvccStorage = new MVCCStorage(lsmTree, tsOracle);
            MockReplicator replicator = new MockReplicator(mvccStorage);
            TxnManager txnManager = new TxnManager(mvccStorage, replicator, tsOracle);

            txnManager.execute(txn -> {
                txn.put("a".getBytes(), "1".getBytes());
                txn.put("b".getBytes(), "2".getBytes());
            });

            txnManager.execute(txn -> {
                txn.put("b".getBytes(), "22".getBytes());
                txn.put("c".getBytes(), "3".getBytes());
            });

            txnManager.execute(txn -> {
                txn.delete("a".getBytes());
                txn.put("c".getBytes(), "33".getBytes());
            });

            assertTrue(mvccStorage.get("a".getBytes()).isEmpty());
            assertArrayEquals("22".getBytes(), mvccStorage.get("b".getBytes()).orElseThrow());
            assertArrayEquals("33".getBytes(), mvccStorage.get("c".getBytes()).orElseThrow());
        } finally {
            lsmTree.close();
        }
    }

    @Test
    void testManualTransactionControl() throws Exception {
        Config config = createConfig("txn-node");
        LSMTree lsmTree = new LSMTree(config);
        try {
            lsmTree.recover();
            TimestampOracle tsOracle = new TimestampOracle();
            MVCCStorage mvccStorage = new MVCCStorage(lsmTree, tsOracle);
            MockReplicator replicator = new MockReplicator(mvccStorage);
            TxnManager txnManager = new TxnManager(mvccStorage, replicator, tsOracle);

            Txn txn = txnManager.begin();
            txn.put("key".getBytes(), "value".getBytes());
            txn.commit();

            assertArrayEquals("value".getBytes(), mvccStorage.get("key".getBytes()).orElseThrow());
        } finally {
            lsmTree.close();
        }
    }
}
