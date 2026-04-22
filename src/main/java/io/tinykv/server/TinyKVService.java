package io.tinykv.server;

import io.tinykv.common.Config;
import io.tinykv.coordinator.CoordinatorClient;
import io.tinykv.raft.*;
import io.tinykv.replication.*;
import io.tinykv.storage.LSMTree;
import io.tinykv.storage.StorageEngine;
import io.tinykv.transaction.*;
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
 */
public class TinyKVService {

    private static final Logger LOG = LoggerFactory.getLogger(TinyKVService.class);

    private final Config config;
    private final int nodeId;
    private final List<Integer> peerIds;

    private StorageEngine storageEngine;
    private RaftNode raftNode;
    private RaftTransport transport;
    private SyncReplicator syncReplicator;
    private AsyncReplicator asyncReplicator;
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
        LOG.info("Starting TinyKV node {}", nodeId);

        // 1. Initialize storage engine
        storageEngine = new LSMTree(config);
        storageEngine.recover();

        // 2. Initialize Raft
        raftNode = new RaftNode(nodeId, peerIds, config,
                new SyncReplicator.LSMTreeStateMachine(storageEngine));

        // 3. Setup transport and wire KV service before starting
        transport = createTransport();
        transport.register(nodeId, raftNode);
        raftNode.setTransport(transport);

        // Wire KV gRPC service into transport before starting
        if (transport instanceof io.tinykv.raft.GrpcTransport) {
            ((io.tinykv.raft.GrpcTransport) transport).setKVService(this);
        }

        transport.start();
        raftNode.start();

        // 4. Initialize replication
        syncReplicator = new SyncReplicator(raftNode, storageEngine, config);
        asyncReplicator = new AsyncReplicator(raftNode, transport);

        // 5. Initialize transaction layer
        tsOracle = new TimestampOracle();
        txnManager = new TxnManager(storageEngine, syncReplicator, tsOracle);

        // 6. Register with coordinator
        if (config.getCoordinatorAddress() != null && !config.getCoordinatorAddress().isEmpty()) {
            registerWithCoordinator();
        }

        started = true;
        LOG.info("TinyKV node {} started successfully", nodeId);
    }

    // ==================== Simple KV API ====================

    /**
     * Put a key-value pair (sync replicated via Raft).
     */
    public void put(byte[] key, byte[] value) throws InterruptedException, TimeoutException {
        syncReplicator.put(key, value);
    }

    /**
     * Get a value by key.
     */
    public Optional<byte[]> get(byte[] key) {
        return syncReplicator.get(key);
    }

    /**
     * Delete a key (sync replicated via Raft).
     */
    public void delete(byte[] key) throws InterruptedException, TimeoutException {
        syncReplicator.delete(key);
    }

    /**
     * Scan keys in [startKey, endKey) range.
     */
    public Iterator<KVEntry> scan(byte[] startKey, byte[] endKey) {
        return new ScanIterator(storageEngine.scan(startKey, endKey));
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

    // ==================== Async Replication API ====================

    /**
     * Add a learner node for async replication.
     */
    public void addLearner(RaftNode learner) {
        asyncReplicator.addLearner(learner);
    }

    /**
     * Get replication lag for a learner.
     */
    public long getLearnerLag(int learnerId) {
        return asyncReplicator.getReplicationLag(learnerId);
    }

    // ==================== Info ====================

    public RaftState getRaftState() {
        return raftNode.getState();
    }

    public int getLeaderId() {
        return raftNode.getLeaderId();
    }

    public long getCommitIndex() {
        return raftNode.getCommitIndex();
    }

    public boolean isLeader() {
        return raftNode.isLeader();
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

        asyncReplicator.stop();
        raftNode.stop();
        transport.stop();

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

        // Register node
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

    private RaftTransport createTransport() {
        // Use gRPC transport for real cluster communication
        if (config.getPeerAddresses() != null && !config.getPeerAddresses().isEmpty()) {
            return new GrpcTransport(config);
        }
        // Fall back to local transport for single-node or testing
        return new LocalTransport();
    }

    private int parseNodeId(Config config) {
        // First check all peer addresses to find our ID
        Map<Integer, String> peerAddresses = config.getAllPeerAddresses();
        String ourAddress = config.getAddress();

        // Find our own node ID
        for (Map.Entry<Integer, String> entry : peerAddresses.entrySet()) {
            if (entry.getValue().equals(ourAddress)) {
                return entry.getKey();
            }
        }

        // Fallback: parse node ID from address format "nodeId:host:port"
        String[] parts = ourAddress.split(":");
        if (parts.length >= 3) {
            try {
                return Integer.parseInt(parts[0]);
            } catch (NumberFormatException e) {
                // Ignore, use default
            }
        }

        // If just "host:port", derive ID from port
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
