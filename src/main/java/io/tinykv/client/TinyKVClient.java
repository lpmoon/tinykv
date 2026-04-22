package io.tinykv.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.tinykv.coordinator.CoordinatorClient;
import io.tinykv.proto.kv.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * TinyKV client with automatic leader discovery.
 *
 * <p>Two discovery modes:
 * <ol>
 *   <li>Via Coordinator: pass --coordinator address, client queries coordinator for leader</li>
 *   <li>Via seed addresses: pass seed node addresses, client probes nodes for leader</li>
 * </ol>
 *
 * <p>Usage (via Coordinator):
 * <pre>
 * TinyKVClient client = new TinyKVClient("localhost:8000", "my-cluster");
 * client.put("key1", "value1".getBytes());
 * byte[] value = client.get("key1").orElse(null);
 * client.close();
 * </pre>
 */
public class TinyKVClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TinyKVClient.class);

    private final String coordinatorAddress;
    private final String clusterName;
    private final List<String> seedAddresses;

    private CoordinatorClient coordinatorClient;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, KVServiceGrpc.KVServiceBlockingStub> stubs = new ConcurrentHashMap<>();

    private volatile String cachedLeaderAddress = null;
    private volatile int cachedLeaderId = -1;
    private long leaderCacheTimestamp = 0;
    private final long leaderCacheTtlMs;

    private final int maxRetries;
    private final long retryDelayMs;

    /**
     * Create a client via Coordinator (recommended).
     * The client will query the coordinator for the leader address.
     */
    public TinyKVClient(String coordinatorAddress, String clusterName) {
        this(coordinatorAddress, clusterName, List.of(), 5000L, 3, 100L);
    }

    /**
     * Create a client via seed addresses (fallback mode).
     * The client will probe seed addresses to find the leader.
     */
    public TinyKVClient(List<String> seedAddresses) {
        this(null, "default", seedAddresses, 5000L, 3, 100L);
    }

    /**
     * Create a client with full control.
     */
    public TinyKVClient(String coordinatorAddress, String clusterName, List<String> seedAddresses,
                        long leaderCacheTtlMs, int maxRetries, long retryDelayMs) {
        this.coordinatorAddress = coordinatorAddress;
        this.clusterName = clusterName;
        this.seedAddresses = new ArrayList<>(seedAddresses);
        this.leaderCacheTtlMs = leaderCacheTtlMs;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    // ==================== Public KV API ====================

    /**
     * Put a key-value pair. Must be called on the leader.
     */
    public void put(byte[] key, byte[] value) {
        executeWithLeaderRedirect(() -> {
            PutRequest req = PutRequest.newBuilder()
                    .setKey(com.google.protobuf.ByteString.copyFrom(key))
                    .setValue(com.google.protobuf.ByteString.copyFrom(value))
                    .build();
            PutResponse resp = getLeaderStub().put(req);
            if (!resp.getOk()) {
                throw new RuntimeException("Put failed");
            }
            return null;
        });
    }

    /**
     * Get a value by key. Can be served by any node (strong read goes to leader).
     */
    public Optional<byte[]> get(byte[] key) {
        return executeWithLeaderRedirect(() -> {
            GetRequest req = GetRequest.newBuilder()
                    .setKey(com.google.protobuf.ByteString.copyFrom(key))
                    .build();
            GetResponse resp = getLeaderStub().get(req);
            if (resp.getFound()) {
                return Optional.of(resp.getValue().toByteArray());
            }
            return Optional.<byte[]>empty();
        });
    }

    /**
     * Get a value by key from any available node (faster but potentially stale).
     */
    public Optional<byte[]> getFromAnyNode(byte[] key) {
        return executeOnAnyNode(stub -> {
            GetRequest req = GetRequest.newBuilder()
                    .setKey(com.google.protobuf.ByteString.copyFrom(key))
                    .build();
            GetResponse resp = stub.get(req);
            if (resp.getFound()) {
                return Optional.of(resp.getValue().toByteArray());
            }
            return Optional.<byte[]>empty();
        });
    }

    /**
     * Delete a key. Must be called on the leader.
     */
    public void delete(byte[] key) {
        executeWithLeaderRedirect(() -> {
            DeleteRequest req = DeleteRequest.newBuilder()
                    .setKey(com.google.protobuf.ByteString.copyFrom(key))
                    .build();
            DeleteResponse resp = getLeaderStub().delete(req);
            if (!resp.getOk()) {
                throw new RuntimeException("Delete failed");
            }
            return null;
        });
    }

    /**
     * Scan keys in [startKey, endKey) range. Must be called on the leader.
     */
    public List<KVEntry> scan(byte[] startKey, byte[] endKey) {
        return executeWithLeaderRedirect(() -> {
            ScanRequest req = ScanRequest.newBuilder()
                    .setStartKey(com.google.protobuf.ByteString.copyFrom(startKey))
                    .setEndKey(com.google.protobuf.ByteString.copyFrom(endKey))
                    .build();
            List<KVEntry> results = new ArrayList<>();
            Iterator<ScanEntry> iter = getLeaderStub().scan(req);
            while (iter.hasNext()) {
                ScanEntry entry = iter.next();
                results.add(new KVEntry(entry.getKey().toByteArray(), entry.getValue().toByteArray()));
            }
            return results;
        });
    }

    // ==================== Cluster Discovery ====================

    /**
     * Get cluster information from coordinator.
     */
    public ClusterInfo getClusterInfo() {
        ensureCoordinatorClient();
        CoordinatorClient.ClusterInfo info = coordinatorClient.getClusterInfo(clusterName);
        return new ClusterInfo(info.leaderId, info.leaderAddress, info.nodes);
    }

    /**
     * Force refresh the cached leader address.
     */
    public void refreshLeader() {
        leaderCacheTimestamp = 0;
        discoverLeader();
    }

    // ==================== Internal ====================

    private <T> T executeWithLeaderRedirect(ThrowingSupplier<T> action) {
        int attempts = 0;
        while (true) {
            try {
                String leader = getOrDiscoverLeader();
                if (leader == null) {
                    throw new NoLeaderException("Cannot find leader in cluster");
                }

                return action.get();

            } catch (LeaderRedirectException e) {
                if (++attempts > maxRetries) {
                    throw new RuntimeException("Max retries exceeded after leader redirects", e);
                }
                LOG.debug("Leader redirect to {}, retrying ({}/{})", e.leaderAddress, attempts, maxRetries);
                cachedLeaderAddress = e.leaderAddress;
                cachedLeaderId = e.leaderId;
                leaderCacheTimestamp = System.currentTimeMillis();
                sleep(retryDelayMs);
            } catch (Exception e) {
                if (++attempts > maxRetries) {
                    throw new RuntimeException("Max retries exceeded", e);
                }
                LOG.debug("Operation failed, retrying ({}/{}): {}", attempts, maxRetries, e.getMessage());
                cachedLeaderAddress = null;
                leaderCacheTimestamp = 0;
                sleep(retryDelayMs);
            }
        }
    }

    private String getOrDiscoverLeader() {
        if (isLeaderCacheValid()) {
            return cachedLeaderAddress;
        }
        return discoverLeader();
    }

    private boolean isLeaderCacheValid() {
        if (cachedLeaderAddress == null) {
            return false;
        }
        return System.currentTimeMillis() - leaderCacheTimestamp < leaderCacheTtlMs;
    }

    private String discoverLeader() {
        // Try coordinator first
        if (coordinatorAddress != null && !coordinatorAddress.isEmpty()) {
            try {
                ensureCoordinatorClient();
                CoordinatorClient.LeaderInfo leader = coordinatorClient.getLeader(clusterName);
                if (leader.hasLeader()) {
                    cachedLeaderAddress = leader.leaderAddress;
                    cachedLeaderId = leader.leaderId;
                    leaderCacheTimestamp = System.currentTimeMillis();
                    LOG.debug("Discovered leader via coordinator: {} (id={})", cachedLeaderAddress, cachedLeaderId);
                    return cachedLeaderAddress;
                }
            } catch (Exception e) {
                LOG.debug("Failed to discover leader via coordinator: {}", e.getMessage());
            }
        }

        // Fallback: probe seed addresses
        if (!seedAddresses.isEmpty()) {
            return discoverViaSeedAddresses();
        }

        return null;
    }

    private String discoverViaSeedAddresses() {
        for (String address : seedAddresses) {
            try {
                ClusterInfoResponse resp = getStub(address).getClusterInfo(
                        ClusterInfoRequest.newBuilder().build());
                if (resp.getLeaderAddress() != null && !resp.getLeaderAddress().isEmpty()) {
                    cachedLeaderAddress = resp.getLeaderAddress();
                    cachedLeaderId = resp.getLeaderId();
                    leaderCacheTimestamp = System.currentTimeMillis();
                    LOG.debug("Discovered leader via seed {}: {}", address, cachedLeaderAddress);
                    return cachedLeaderAddress;
                }
            } catch (Exception e) {
                LOG.debug("Seed {} failed: {}", address, e.getMessage());
            }
        }
        return null;
    }

    private <T> T executeOnAnyNode(ThrowingExecutor<T> executor) {
        List<String> targets = new ArrayList<>();
        if (cachedLeaderAddress != null) {
            targets.add(cachedLeaderAddress);
        }
        targets.addAll(seedAddresses);
        Collections.shuffle(targets);

        for (String address : targets) {
            try {
                return executor.execute(getStub(address));
            } catch (Exception e) {
                LOG.debug("Node {} failed: {}", address, e.getMessage());
            }
        }
        throw new RuntimeException("All nodes unavailable");
    }

    private void ensureCoordinatorClient() {
        if (coordinatorClient == null) {
            coordinatorClient = new CoordinatorClient(coordinatorAddress);
        }
    }

    private KVServiceGrpc.KVServiceBlockingStub getLeaderStub() {
        if (cachedLeaderAddress == null) {
            discoverLeader();
        }
        if (cachedLeaderAddress == null) {
            throw new NoLeaderException("No leader found");
        }
        return getStub(cachedLeaderAddress);
    }

    private synchronized KVServiceGrpc.KVServiceBlockingStub getStub(String address) {
        KVServiceGrpc.KVServiceBlockingStub stub = stubs.get(address);
        if (stub == null) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address)
                    .usePlaintext()
                    .build();
            channels.put(address, channel);
            stub = KVServiceGrpc.newBlockingStub(channel);
            stubs.put(address, stub);
        }
        return stub;
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        if (coordinatorClient != null) {
            coordinatorClient.close();
        }
        for (ManagedChannel ch : channels.values()) {
            ch.shutdown();
        }
        try {
            for (ManagedChannel ch : channels.values()) {
                ch.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("TinyKVClient closed");
    }

    // ==================== Types ====================

    public record KVEntry(byte[] key, byte[] value) {}

    public record ClusterInfo(int leaderId, String leaderAddress,
                             List<io.tinykv.proto.coordinator.NodeInfo> nodes) {}

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingExecutor<T> {
        T execute(KVServiceGrpc.KVServiceBlockingStub stub) throws Exception;
    }

    public static class LeaderRedirectException extends RuntimeException {
        public final String leaderAddress;
        public final int leaderId;

        LeaderRedirectException(String leaderAddress, int leaderId) {
            super("Not leader, redirect to: " + leaderAddress);
            this.leaderAddress = leaderAddress;
            this.leaderId = leaderId;
        }
    }

    public static class NoLeaderException extends RuntimeException {
        NoLeaderException(String msg) {
            super(msg);
        }
    }
}
