package io.tinykv.server;

import io.tinykv.common.Config;
import io.tinykv.coordinator.CoordinatorClient;
import io.tinykv.raft.*;
import io.tinykv.replication.async.AsyncReplicator;
import io.tinykv.replication.async.GrpcReplicationTransport;
import io.tinykv.replication.async.LocalReplicationTransport;
import io.tinykv.replication.async.ReplicationTransport;
import io.tinykv.replication.common.ReplicateResult;
import io.tinykv.replication.common.Replicator;
import io.tinykv.replication.sync.SyncReplicator;
import io.tinykv.storage.LSMTree;
import io.tinykv.storage.MVCCStorage;
import io.tinykv.transaction.TimestampOracle;
import io.tinykv.transaction.Txn;
import io.tinykv.transaction.TxnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * TinyKV server: the top-level orchestrator that wires together
 * storage engine, Raft, replication, and transaction layers.
 *
 * Provides two access modes:
 * 1. Simple KV: put/get/delete/scan (goes through Raft for writes)
 * 2. Transactional: begin/commit/abort with MVCC
 *
 * Replication modes (configured via replicationMode):
 * - "sync": writes go through Raft, wait for majority before returning
 * - "async": writes apply immediately, replicate in background (Redis-style)
 */
public class TinyKVService {

    private static final Logger LOG = LoggerFactory.getLogger(TinyKVService.class);

    private final Config config;
    private final int nodeId;
    private final List<Integer> peerIds;

    private LSMTree storageEngine;
    private MVCCStorage mvccStorage;
    private RaftNode raftNode;
    private RaftTransport raftTransport;
    private Replicator replicator;
    private AsyncReplicator asyncReplicator; // null in sync mode
    private ReplicationTransport replicationTransport; // null in sync mode
    private TxnManager txnManager;
    private TimestampOracle tsOracle;

    private CoordinatorClient coordinatorClient;
    private ScheduledExecutorService heartbeatScheduler;
    private ScheduledFuture<?> heartbeatTask;

    private volatile boolean started = false;

    public TinyKVService(Config config) {
        this.config = config;
        this.nodeId = parseNodeId(config);
        this.peerIds = parsePeerIds(config);
    }

    /**
     * Start the TinyKV service.
     */
    public void start() throws IOException {
        LOG.info("Starting TinyKV node {} (replicationMode={})", nodeId, config.getReplicationMode());

        // 1. Initialize storage engine + MVCC layer
        storageEngine = new LSMTree(config);
        storageEngine.recover();
        tsOracle = new TimestampOracle();
        mvccStorage = new MVCCStorage(storageEngine, tsOracle);

        // 2. Initialize replicator based on mode.
        String mode = config.getReplicationMode().toLowerCase();
        if ("async".equals(mode)) {
            initAsyncReplication();
        } else {
            initRaft();
            initSyncReplication();
        }

        // 3. Initialize transaction layer
        txnManager = new TxnManager(mvccStorage,
                (replicator instanceof SyncReplicator sr) ? sr : null,
                tsOracle);

        // 4. Register with coordinator
        if (config.getCoordinatorAddress() != null && !config.getCoordinatorAddress().isEmpty()) {
            registerWithCoordinator();
        }

        started = true;
        LOG.info("TinyKV node {} started successfully (mode={})", nodeId, mode);
    }

    private void initRaft() throws IOException {
        raftNode = new RaftNode(nodeId, peerIds, config,
                new SyncReplicator.LSMTreeStateMachine(mvccStorage));

        raftTransport = createRaftTransport();
        raftTransport.register(nodeId, raftNode);
        raftNode.setTransport(raftTransport);

        // Wire KV gRPC service into Raft transport before starting
        if (raftTransport instanceof io.tinykv.raft.GrpcTransport) {
            ((io.tinykv.raft.GrpcTransport) raftTransport).setKVService(this);
        }

        raftTransport.start();
        raftNode.start();
    }

    private void initSyncReplication() {
        replicator = new SyncReplicator(raftNode, mvccStorage, config);
        replicator.start();
    }

    private void initAsyncReplication() {
        replicationTransport = createReplicationTransport();
        asyncReplicator = new AsyncReplicator(
                nodeId, peerIds,
                config.getDataDir() + "/async-repl-" + nodeId,
                new SyncReplicator.LSMTreeStateMachine(mvccStorage),
                replicationTransport,
                config.getRaftHeartbeatIntervalMs(),
                config.getRaftElectionTimeoutMs()
        );
        replicationTransport.register(nodeId, asyncReplicator);
        replicationTransport.start();
        asyncReplicator.start();
        replicator = asyncReplicator;
    }

    // ==================== Simple KV API ====================

    /**
     * Put a key-value pair using the configured replication mode.
     * In async mode with waitReplicas > 0, automatically waits for N replicas.
     */
    public CompletableFuture<ReplicateResult> put(byte[] key, byte[] value) {
        CompletableFuture<ReplicateResult> future = replicator.put(key, value);
        if (config.getWaitReplicas() > 0 && asyncReplicator != null) {
            return future.thenApply(result -> {
                if (result.success()) {
                    asyncReplicator.wait(result.index(), config.getWaitReplicas(), 5000);
                }
                return result;
            });
        }
        return future;
    }

    /**
     * Get a value by key.
     */
    public Optional<byte[]> get(byte[] key) {
        return replicator.get(key);
    }

    /**
     * Delete a key using the configured replication mode.
     * In async mode with waitReplicas > 0, automatically waits for N replicas.
     */
    public CompletableFuture<ReplicateResult> delete(byte[] key) {
        CompletableFuture<ReplicateResult> future = replicator.delete(key);
        if (config.getWaitReplicas() > 0 && asyncReplicator != null) {
            return future.thenApply(result -> {
                if (result.success()) {
                    asyncReplicator.wait(result.index(), config.getWaitReplicas(), 5000);
                }
                return result;
            });
        }
        return future;
    }

    /**
     * Scan keys in [startKey, endKey) range.
     */
    public Iterator<KVEntry> scan(byte[] startKey, byte[] endKey) {
        return new ScanIterator(storageEngine.scan(startKey, endKey));
    }

    // ==================== Replication Info ====================

    /**
     * Get the current replication mode.
     */
    public String getReplicationMode() {
        return asyncReplicator != null ? "async" : "sync";
    }

    // ==================== Transaction API ====================

    /**
     * Begin a new MVCC transaction.
     */
    public Txn beginTxn() {
        return txnManager.begin();
    }

    /**
     * Execute a transaction with auto-commit/abort.
     */
    public void executeTxn(TxnManager.TxnCallback callback) throws Exception {
        txnManager.execute(callback);
    }

    // ==================== Info ====================

    public RaftState getRaftState() {
        // async 模式没有 RaftNode，用 FOLLOWER 占位（由 AsyncReplicator 自己管选举状态）
        return raftNode != null ? raftNode.getState() : RaftState.FOLLOWER;
    }

    public int getLeaderId() {
        // AsyncReplicator 没有暴露 getLeaderId()，async 模式下返回 -1（未知）
        return raftNode != null ? raftNode.getLeaderId() : -1;
    }

    public long getCommitIndex() {
        return raftNode != null ? raftNode.getCommitIndex() :
                (asyncReplicator != null ? asyncReplicator.getCommitIndex() : 0);
    }

    public boolean isLeader() {
        return raftNode != null ? raftNode.isLeader() :
                (asyncReplicator != null && asyncReplicator.isLeader());
    }

    // ==================== Lifecycle ====================

    public void stop() {
        LOG.info("Stopping TinyKV node {}", nodeId);

        // Stop heartbeat
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdown();
        }

        // Close coordinator client
        if (coordinatorClient != null) {
            coordinatorClient.close();
        }

        // Stop replicators
        if (asyncReplicator != null) {
            asyncReplicator.stop();
        }
        if (replicationTransport != null) {
            replicationTransport.stop();
        }

        // async 模式下 raftNode/raftTransport 未初始化，跳过
        if (raftNode != null) {
            raftNode.stop();
        }
        if (raftTransport != null) {
            raftTransport.stop();
        }

        try {
            storageEngine.close();
        } catch (IOException e) {
            LOG.error("Error closing storage engine", e);
        }

        started = false;
        LOG.info("TinyKV node {} stopped", nodeId);
    }

    // ==================== Internal ====================

    private void registerWithCoordinator() {
        LOG.info("Registering with coordinator at {}", config.getCoordinatorAddress());

        coordinatorClient = new CoordinatorClient(config.getCoordinatorAddress());

        boolean success = coordinatorClient.registerNode(
                config.getClusterName(),
                nodeId,
                config.getAddress(),
                raftNode.isLeader()
        );

        if (success) {
            LOG.info("Successfully registered with coordinator");
            startHeartbeat();
        } else {
            LOG.warn("Failed to register with coordinator");
        }
    }

    private void startHeartbeat() {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r ->
                new Thread(r, "coordinator-heartbeat"));
        heartbeatTask = heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                if (started) {
                    coordinatorClient.heartbeat(
                            config.getClusterName(),
                            nodeId,
                            raftNode.isLeader(),
                            raftNode.getCurrentTerm()
                    );
                }
            } catch (Exception e) {
                LOG.debug("Heartbeat failed: {}", e.getMessage());
            }
        }, 1000, 3000, TimeUnit.MILLISECONDS);
    }

    private RaftTransport createRaftTransport() {
        if (config.getPeerAddresses() != null && !config.getPeerAddresses().isEmpty()) {
            return new GrpcTransport(config);
        }
        return new LocalTransport();
    }

    private ReplicationTransport createReplicationTransport() {
        if (config.getPeerAddresses() != null && !config.getPeerAddresses().isEmpty()) {
            return new GrpcReplicationTransport(config);
        }
        return new LocalReplicationTransport();
    }

    private int parseNodeId(Config config) {
        Map<Integer, String> peerAddresses = config.getAllPeerAddresses();
        String ourAddress = config.getAddress();

        for (Map.Entry<Integer, String> entry : peerAddresses.entrySet()) {
            if (entry.getValue().equals(ourAddress)) {
                return entry.getKey();
            }
        }

        String[] parts = ourAddress.split(":");
        if (parts.length >= 3) {
            try {
                return Integer.parseInt(parts[0]);
            } catch (NumberFormatException e) {
                // Ignore, use default
            }
        }

        return Integer.parseInt(parts[parts.length - 1]) % 100;
    }

    private List<Integer> parsePeerIds(Config config) {
        Map<Integer, String> peerAddresses = config.getAllPeerAddresses();
        int ourNodeId = parseNodeId(config);
        List<Integer> peerIds = new ArrayList<>();
        for (Integer id : peerAddresses.keySet()) {
            if (id != ourNodeId) {
                peerIds.add(id);
            }
        }
        return peerIds;
    }

    // ==================== Helper Classes ====================

    public record KVEntry(byte[] key, byte[] value) {}

    /**
     * Wraps a KVIterator into a Java Iterator.
     */
    static class ScanIterator implements Iterator<KVEntry>, AutoCloseable {

        private final io.tinykv.storage.KVIterator inner;

        ScanIterator(io.tinykv.storage.KVIterator inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public KVEntry next() {
            inner.next();
            return new KVEntry(inner.key(), inner.value());
        }

        @Override
        public void close() {
            try { inner.close(); } catch (Exception ignored) {}
        }
    }
}
