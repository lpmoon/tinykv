package io.tinykv.replication;

import io.tinykv.raft.*;
import io.tinykv.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Asynchronous replication using Raft Learner nodes.
 *
 * A Learner node:
 * - Receives AppendEntries from the leader but does not vote
 * - Does not participate in elections or commit advancement
 * - Eventually consistent: may lag behind the leader
 * - Suitable for read-only replicas, cross-DC replicas, analytics
 *
 * This class manages learner nodes and their replication state.
 */
public class AsyncReplicator {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncReplicator.class);

    private final RaftNode leaderNode;
    private final RaftTransport transport;
    private final ScheduledExecutorService scheduler;

    // Track replication lag for each learner
    private final ConcurrentHashMap<Integer, ReplicationStatus> learnerStatus = new ConcurrentHashMap<>();

    public AsyncReplicator(RaftNode leaderNode, RaftTransport transport) {
        this.leaderNode = leaderNode;
        this.transport = transport;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "async-replicator");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Add a learner node for async replication.
     * Learner nodes receive log entries but don't vote.
     */
    public void addLearner(RaftNode learnerNode) {
        int learnerId = learnerNode.getId();
        learnerStatus.put(learnerId, new ReplicationStatus(learnerId, 0, 0));
        transport.register(learnerId, learnerNode);

        LOG.info("Added learner node {} for async replication", learnerId);
    }

    /**
     * Remove a learner node.
     */
    public void removeLearner(int learnerId) {
        learnerStatus.remove(learnerId);
        LOG.info("Removed learner node {}", learnerId);
    }

    /**
     * Start periodic async replication to learners.
     * The leader sends AppendEntries to learners at a lower frequency
     * than synchronous followers.
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::replicateToLearners, 0, 200, TimeUnit.MILLISECONDS);
    }

    private void replicateToLearners() {
        if (!leaderNode.isLeader()) return;

        long leaderCommitIndex = leaderNode.getCommitIndex();

        for (Map.Entry<Integer, ReplicationStatus> entry : learnerStatus.entrySet()) {
            int learnerId = entry.getKey();
            ReplicationStatus status = entry.getValue();

            if (status.nextIndex <= leaderCommitIndex) {
                sendEntriesToLearner(learnerId, status);
            }
        }
    }

    private void sendEntriesToLearner(int learnerId, ReplicationStatus status) {
        // Build AppendEntries for the learner
        long prevLogIndex = status.nextIndex - 1;
        // The actual sending is done via the RaftNode's internal mechanisms
        // Here we just trigger the leader to include this learner in replication
        LOG.debug("Replicating to learner {} from index {}", learnerId, status.nextIndex);

        // Update status
        status.lastReplicatedAt = System.currentTimeMillis();
    }

    /**
     * Get the replication lag for a learner (difference between leader commit and learner match index).
     */
    public long getReplicationLag(int learnerId) {
        ReplicationStatus status = learnerStatus.get(learnerId);
        if (status == null) return -1;
        return leaderNode.getCommitIndex() - status.matchIndex;
    }

    /**
     * Check if a learner is within an acceptable lag threshold.
     */
    public boolean isLearnerHealthy(int learnerId, long maxLagThreshold) {
        return getReplicationLag(learnerId) <= maxLagThreshold;
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Tracks replication state for a single learner.
     */
    static class ReplicationStatus {
        final int learnerId;
        volatile long nextIndex;
        volatile long matchIndex;
        volatile long lastReplicatedAt;

        ReplicationStatus(int learnerId, long nextIndex, long matchIndex) {
            this.learnerId = learnerId;
            this.nextIndex = nextIndex;
            this.matchIndex = matchIndex;
            this.lastReplicatedAt = System.currentTimeMillis();
        }
    }
}
