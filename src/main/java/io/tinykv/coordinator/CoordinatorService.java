package io.tinykv.coordinator;

import io.tinykv.proto.coordinator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Coordinator service for cluster management and leader discovery.
 *
 * Responsibilities:
 * - Node registration
 * - Heartbeat tracking
 * - Leader discovery for clients
 */
public class CoordinatorService {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorService.class);

    private final String bindAddress;
    private final int port;
    private final long heartbeatTimeoutMs;

    // clusterName -> ClusterState
    private final Map<String, ClusterState> clusters = new ConcurrentHashMap<>();

    // Cleanup expired nodes periodically
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "coordinator-cleanup"));

    public CoordinatorService(String bindAddress, int port, long heartbeatTimeoutMs) {
        this.bindAddress = bindAddress;
        this.port = port;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public void start() {
        // Start cleanup task
        scheduler.scheduleAtFixedRate(this::cleanupExpiredNodes, 5000, 5000,
                TimeUnit.MILLISECONDS);
        LOG.info("Coordinator started on {}:{}", bindAddress, port);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Coordinator stopped");
    }

    // ── RPC Handlers ────────────────────────────────────────

    public RegisterNodeResponse registerNode(RegisterNodeRequest req) {
        LOG.info("RegisterNode: cluster={}, nodeId={}, addr={}, isLeader={}",
                req.getClusterName(), req.getNodeId(), req.getNodeAddress(), req.getIsLeader());

        ClusterState cluster = clusters.computeIfAbsent(req.getClusterName(), k -> new ClusterState());
        NodeInfo node = new NodeInfo(
                req.getNodeId(),
                req.getNodeAddress(),
                req.getIsLeader(),
                System.currentTimeMillis()
        );

        boolean updated = cluster.addOrUpdateNode(node);

        if (req.getIsLeader()) {
            cluster.setLeader(node);
            LOG.info("Leader registered for cluster {}: nodeId={}, addr={}",
                    req.getClusterName(), req.getNodeId(), req.getNodeAddress());
        }

        return RegisterNodeResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Node registered successfully")
                .build();
    }

    public HeartbeatResponse heartbeat(HeartbeatRequest req) {
        String clusterName = req.getClusterName();
        int nodeId = req.getNodeId();

        ClusterState cluster = clusters.get(clusterName);
        if (cluster == null) {
            return HeartbeatResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Cluster not found: " + clusterName)
                    .build();
        }

        NodeInfo node = cluster.getNode(nodeId);
        if (node == null) {
            return HeartbeatResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Node not registered")
                    .build();
        }

        // Update heartbeat and leader status
        node.lastHeartbeat = System.currentTimeMillis();
        node.isLeader = req.getIsLeader();

        if (req.getIsLeader()) {
            cluster.setLeader(node);
        }

        return HeartbeatResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Heartbeat received")
                .build();
    }

    public GetLeaderResponse getLeader(GetLeaderRequest req) {
        String clusterName = req.getClusterName();
        LOG.debug("GetLeader: cluster={}", clusterName);

        ClusterState cluster = clusters.get(clusterName);
        if (cluster == null || !cluster.hasLeader()) {
            return GetLeaderResponse.newBuilder()
                    .setHasLeader(false)
                    .setClusterName(clusterName)
                    .build();
        }

        NodeInfo leader = cluster.getLeader();
        return GetLeaderResponse.newBuilder()
                .setHasLeader(true)
                .setLeaderId(leader.nodeId)
                .setLeaderAddress(leader.address)
                .setClusterName(clusterName)
                .build();
    }

    public GetClusterInfoResponse getClusterInfo(GetClusterInfoRequest req) {
        String clusterName = req.getClusterName();
        LOG.debug("GetClusterInfo: cluster={}", clusterName);

        ClusterState cluster = clusters.get(clusterName);
        if (cluster == null) {
            return GetClusterInfoResponse.newBuilder()
                    .setClusterName(clusterName)
                    .build();
        }

        GetClusterInfoResponse.Builder builder = GetClusterInfoResponse.newBuilder()
                .setClusterName(clusterName);

        if (cluster.hasLeader()) {
            NodeInfo leader = cluster.getLeader();
            builder.setLeaderId(leader.nodeId);
            builder.setLeaderAddress(leader.address);
        }

        for (NodeInfo node : cluster.getAllNodes()) {
            builder.addNodes(NodeInfoProto.newBuilder()
                    .setNodeId(node.nodeId)
                    .setAddress(node.address)
                    .setIsLeader(node.isLeader)
                    .setLastHeartbeat(node.lastHeartbeat)
                    .build());
        }

        return builder.build();
    }

    // ── Internal ────────────────────────────────────────────

    private void cleanupExpiredNodes() {
        long now = System.currentTimeMillis();
        for (ClusterState cluster : clusters.values()) {
            cluster.removeExpiredNodes(now, heartbeatTimeoutMs);
        }
    }

    // ── Types ───────────────────────────────────────────────

    public static class NodeInfo {
        public int nodeId;
        public String address;
        public volatile boolean isLeader;
        public volatile long lastHeartbeat;

        NodeInfo(int nodeId, String address, boolean isLeader, long lastHeartbeat) {
            this.nodeId = nodeId;
            this.address = address;
            this.isLeader = isLeader;
            this.lastHeartbeat = lastHeartbeat;
        }
    }

    public static class NodeInfoProto {
        public int nodeId;
        public String address;
        public boolean isLeader;
        public long lastHeartbeat;

        public static NodeInfoProto newBuilder() {
            return new NodeInfoProto();
        }

        public NodeInfoProto setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public NodeInfoProto setAddress(String address) {
            this.address = address;
            return this;
        }

        public NodeInfoProto setIsLeader(boolean isLeader) {
            this.isLeader = isLeader;
            return this;
        }

        public NodeInfoProto setLastHeartbeat(long lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
            return this;
        }

        public io.tinykv.proto.coordinator.NodeInfo build() {
            return io.tinykv.proto.coordinator.NodeInfo.newBuilder()
                    .setNodeId(nodeId)
                    .setAddress(address)
                    .setIsLeader(isLeader)
                    .setLastHeartbeat(lastHeartbeat)
                    .build();
        }
    }

    private static class ClusterState {
        private final Map<Integer, NodeInfo> nodes = new ConcurrentHashMap<>();
        private volatile NodeInfo leader;

        synchronized boolean addOrUpdateNode(NodeInfo node) {
            NodeInfo existing = nodes.get(node.nodeId);
            nodes.put(node.nodeId, node);
            return existing != null;
        }

        NodeInfo getNode(int nodeId) {
            return nodes.get(nodeId);
        }

        List<NodeInfo> getAllNodes() {
            return new ArrayList<>(nodes.values());
        }

        synchronized void setLeader(NodeInfo leader) {
            // Demote existing leader
            if (this.leader != null && this.leader.nodeId != leader.nodeId) {
                NodeInfo oldLeader = nodes.get(this.leader.nodeId);
                if (oldLeader != null) {
                    oldLeader.isLeader = false;
                }
            }
            this.leader = leader;
        }

        NodeInfo getLeader() {
            return leader;
        }

        boolean hasLeader() {
            return leader != null;
        }

        synchronized void removeExpiredNodes(long now, long timeoutMs) {
            List<Integer> expired = new ArrayList<>();
            for (Map.Entry<Integer, NodeInfo> entry : nodes.entrySet()) {
                if (now - entry.getValue().lastHeartbeat > timeoutMs) {
                    expired.add(entry.getKey());
                }
            }
            for (Integer nodeId : expired) {
                NodeInfo removed = nodes.remove(nodeId);
                if (removed != null && removed.isLeader) {
                    LOG.warn("Leader node {} expired, clearing leader", nodeId);
                    if (this.leader != null && this.leader.nodeId == nodeId) {
                        this.leader = null;
                    }
                }
                LOG.debug("Removed expired node: {}", nodeId);
            }
        }
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public int getPort() {
        return port;
    }
}
