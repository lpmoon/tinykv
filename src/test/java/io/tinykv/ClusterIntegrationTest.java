package io.tinykv;

import io.tinykv.raft.RaftNode;
import io.tinykv.raft.StateMachine;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for multi-node TinyKV Raft cluster.
 */
public class ClusterIntegrationTest extends IntegrationTestBase {

    static class TestStateMachine implements StateMachine {
        private final Map<String, String> data = new HashMap<>();
        private int applyCount = 0;

        @Override
        public byte[] apply(byte[] command) {
            applyCount++;
            String cmd = new String(command);
            if (cmd.startsWith("PUT:")) {
                String[] parts = cmd.split(":", 3);
                if (parts.length == 3) {
                    data.put(parts[1], parts[2]);
                }
            } else if (cmd.startsWith("DELETE:")) {
                String[] parts = cmd.split(":", 2);
                if (parts.length == 2) {
                    data.remove(parts[1]);
                }
            }
            return null;
        }

        @Override
        public byte[] snapshot() {
            return new byte[0];
        }

        @Override
        public void restore(byte[] snapshot) {
        }

        public Optional<String> get(String key) {
            return Optional.ofNullable(data.get(key));
        }

        public int getApplyCount() {
            return applyCount;
        }
    }

    @Test
    void testSingleNodeCluster() throws Exception {
        try (TestCluster cluster = new TestCluster(testDir + "/cluster", 1)) {
            cluster.start();
            assertTrue(cluster.waitForLeader(30000));
            assertTrue(cluster.getNodes().get(0).isLeader());
        }
    }
}
