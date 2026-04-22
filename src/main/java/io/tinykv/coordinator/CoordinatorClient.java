package io.tinykv.coordinator;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.tinykv.proto.coordinator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client for interacting with the Coordinator service.
 * Used by both TinyKV nodes (for registration and heartbeat)
 * and by TinyKV clients (for leader discovery).
 */
public class CoordinatorClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorClient.class);

    private final String coordinatorAddress;
    private final ManagedChannel channel;
    private final CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub;

    public CoordinatorClient(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
        this.channel = ManagedChannelBuilder.forTarget(coordinatorAddress)
                .usePlaintext()
                .build();
        this.stub = CoordinatorServiceGrpc.newBlockingStub(channel);
    }

    // ── Node Operations ────────────────────────────────────

    /**
     * Register a node with the coordinator.
     */
    public boolean registerNode(String clusterName, int nodeId, String nodeAddress, boolean isLeader) {
        RegisterNodeRequest req = RegisterNodeRequest.newBuilder()
                .setClusterName(clusterName)
                .setNodeId(nodeId)
                .setNodeAddress(nodeAddress)
                .setIsLeader(isLeader)
                .build();

        try {
            RegisterNodeResponse resp = stub.registerNode(req);
            if (resp.getSuccess()) {
                LOG.debug("Node {} registered successfully to cluster {}", nodeId, clusterName);
            } else {
                LOG.warn("Node registration failed: {}", resp.getMessage());
            }
            return resp.getSuccess();
        } catch (Exception e) {
            LOG.error("Failed to register node {} to coordinator: {}", nodeId, e.getMessage());
            return false;
        }
    }

    /**
     * Send heartbeat to the coordinator.
     */
    public boolean heartbeat(String clusterName, int nodeId, boolean isLeader, long term) {
        HeartbeatRequest req = HeartbeatRequest.newBuilder()
                .setClusterName(clusterName)
                .setNodeId(nodeId)
                .setIsLeader(isLeader)
                .setTerm(term)
                .build();

        try {
            HeartbeatResponse resp = stub.heartbeat(req);
            return resp.getSuccess();
        } catch (Exception e) {
            LOG.debug("Heartbeat failed: {}", e.getMessage());
            return false;
        }
    }

    // ── Client Operations ──────────────────────────────────

    /**
     * Get the leader address for a cluster.
     */
    public String getLeaderAddress(String clusterName) {
        GetLeaderRequest req = GetLeaderRequest.newBuilder()
                .setClusterName(clusterName)
                .build();

        try {
            GetLeaderResponse resp = stub.getLeader(req);
            if (resp.getHasLeader()) {
                return resp.getLeaderAddress();
            }
            return null;
        } catch (Exception e) {
            LOG.error("Failed to get leader for cluster {}: {}", clusterName, e.getMessage());
            return null;
        }
    }

    /**
     * Get leader info (id and address) for a cluster.
     */
    public LeaderInfo getLeader(String clusterName) {
        GetLeaderRequest req = GetLeaderRequest.newBuilder()
                .setClusterName(clusterName)
                .build();

        try {
            GetLeaderResponse resp = stub.getLeader(req);
            if (resp.getHasLeader()) {
                return new LeaderInfo(resp.getLeaderId(), resp.getLeaderAddress());
            }
            return LeaderInfo.NO_LEADER;
        } catch (Exception e) {
            LOG.error("Failed to get leader for cluster {}: {}", clusterName, e.getMessage());
            return LeaderInfo.ERROR;
        }
    }

    /**
     * Get full cluster info.
     */
    public ClusterInfo getClusterInfo(String clusterName) {
        GetClusterInfoRequest req = GetClusterInfoRequest.newBuilder()
                .setClusterName(clusterName)
                .build();

        try {
            GetClusterInfoResponse resp = stub.getClusterInfo(req);
            return new ClusterInfo(
                    resp.getClusterName(),
                    resp.getLeaderId(),
                    resp.getLeaderAddress(),
                    resp.getNodesList()
            );
        } catch (Exception e) {
            LOG.error("Failed to get cluster info for {}: {}", clusterName, e.getMessage());
            return new ClusterInfo(clusterName);
        }
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String getCoordinatorAddress() {
        return coordinatorAddress;
    }

    // ── Types ──────────────────────────────────────────────

    public static class LeaderInfo {
        public static final LeaderInfo NO_LEADER = new LeaderInfo(-1, null);
        public static final LeaderInfo ERROR = new LeaderInfo(-2, null);

        public final int leaderId;
        public final String leaderAddress;

        public LeaderInfo(int leaderId, String leaderAddress) {
            this.leaderId = leaderId;
            this.leaderAddress = leaderAddress;
        }

        public boolean hasLeader() {
            return leaderId >= 0 && leaderAddress != null;
        }

        @Override
        public String toString() {
            if (leaderId == -1) return "NO_LEADER";
            if (leaderId == -2) return "ERROR";
            return "Leader{id=" + leaderId + ", address=" + leaderAddress + "}";
        }
    }

    public static class ClusterInfo {
        public final String clusterName;
        public final int leaderId;
        public final String leaderAddress;
        public final List<io.tinykv.proto.coordinator.NodeInfo> nodes;

        public ClusterInfo(String clusterName) {
            this(clusterName, -1, "", List.of());
        }

        public ClusterInfo(String clusterName, int leaderId, String leaderAddress,
                          List<io.tinykv.proto.coordinator.NodeInfo> nodes) {
            this.clusterName = clusterName;
            this.leaderId = leaderId;
            this.leaderAddress = leaderAddress;
            this.nodes = nodes;
        }

        public boolean hasLeader() {
            return leaderId >= 0 && leaderAddress != null;
        }
    }
}
