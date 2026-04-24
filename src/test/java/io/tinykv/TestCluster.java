package io.tinykv;

import io.tinykv.common.Config;
import io.tinykv.raft.LocalTransport;
import io.tinykv.raft.RaftNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test cluster for integration testing using Raft directly.
 */
public class TestCluster implements AutoCloseable {

    private final String baseDir;
    private final int nodeCount;

    private final List<RaftNode> nodes = new ArrayList<>();
    private LocalTransport transport;

    public TestCluster(String baseDir, int nodeCount) {
        this.baseDir = baseDir;
        this.nodeCount = nodeCount;
    }

    public void start() throws IOException {
        List<Integer> allNodeIds = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            allNodeIds.add(i);
        }

        transport = new LocalTransport();

        for (int i = 0; i < nodeCount; i++) {
            List<Integer> peerIds = new ArrayList<>();
            for (int j = 0; j < nodeCount; j++) {
                if (j != i) {
                    peerIds.add(j);
                }
            }

            Config config = new Config()
                    .setDataDir(baseDir + "/node-" + i)
                    .setRaftElectionTimeoutMs(300)
                    .setRaftHeartbeatIntervalMs(50);

            ClusterIntegrationTest.TestStateMachine stateMachine = new ClusterIntegrationTest.TestStateMachine();
            RaftNode node = new RaftNode(i, peerIds, config, stateMachine);
            nodes.add(node);
            transport.register(i, node);
        }

        transport.start();

        for (RaftNode node : nodes) {
            node.start();
        }
    }

    public List<RaftNode> getNodes() {
        return nodes;
    }

    public RaftNode getLeader() {
        for (RaftNode node : nodes) {
            if (node.isLeader()) {
                return node;
            }
        }
        return null;
    }

    public boolean waitForLeader(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (getLeader() != null) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    @Override
    public void close() {
        for (RaftNode node : nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                // Ignore
            }
        }
        if (transport != null) {
            transport.stop();
        }
    }
}
