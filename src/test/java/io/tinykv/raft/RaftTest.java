package io.tinykv.raft;

import io.tinykv.common.Config;
import io.tinykv.raft.*;
import io.tinykv.storage.LSMTree;
import io.tinykv.storage.StorageEngine;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class RaftTest {

    private static final String TEST_DIR = "/tmp/tinykv-test-raft";
    private List<RaftNode> nodes;
    private LocalTransport transport;

    @BeforeEach
    void setUp() throws IOException {
        cleanup();
        transport = new LocalTransport();
        nodes = new ArrayList<>();
    }

    @AfterEach
    void tearDown() throws IOException {
        for (RaftNode node : nodes) {
            node.stop();
        }
        transport.stop();
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

    private RaftNode createNode(int id, List<Integer> peers) throws IOException {
        Config config = new Config().setDataDir(TEST_DIR + "/node-" + id)
                .setRaftElectionTimeoutMs(500)
                .setRaftHeartbeatIntervalMs(100);

        CountingStateMachine sm = new CountingStateMachine();
        RaftNode node = new RaftNode(id, peers, config, sm);
        transport.register(id, node);
        node.setTransport(transport);
        nodes.add(node);
        return node;
    }

    @Test
    void testLeaderElection() throws IOException, InterruptedException {
        List<Integer> peers = List.of(1, 2);
        RaftNode node0 = createNode(0, peers);
        RaftNode node1 = createNode(1, List.of(0, 2));
        RaftNode node2 = createNode(2, List.of(0, 1));

        node0.start();
        node1.start();
        node2.start();
        transport.start();

        // Wait for election
        Thread.sleep(2000);

        // One of the nodes should be leader
        long leaders = nodes.stream().filter(RaftNode::isLeader).count();
        assertEquals(1, leaders, "Exactly one leader should be elected");
    }

    @Test
    void testSingleNode() throws IOException, InterruptedException {
        RaftNode node0 = createNode(0, List.of());
        node0.start();
        transport.start();

        Thread.sleep(1500);

        // Single node should become leader
        assertTrue(node0.isLeader(), "Single node should become leader");
    }

    /**
     * Simple state machine that counts applied commands.
     */
    static class CountingStateMachine implements StateMachine {
        private int applyCount = 0;
        private final List<byte[]> appliedCommands = new ArrayList<>();

        @Override
        public byte[] apply(byte[] command) {
            applyCount++;
            appliedCommands.add(command);
            return null;
        }

        @Override
        public byte[] snapshot() {
            return new byte[0];
        }

        @Override
        public void restore(byte[] snapshot) {}

        public int getApplyCount() { return applyCount; }
    }
}
