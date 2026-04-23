package io.tinykv.replication.async;

import io.tinykv.raft.StateMachine;
import io.tinykv.replication.common.CommandCodec;
import io.tinykv.replication.common.ReplicateResult;
import io.tinykv.replication.common.Replicator;
import io.tinykv.replication.common.WriteOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis-style asynchronous replication with simple epoch-based leader election.
 *
 * <h3>Leader Election</h3>
 * - Epoch mechanism: monotonically increasing, similar to Raft term
 * - Follower times out → epoch++ → requests votes → majority → becomes leader
 * - Leader sends periodic heartbeats to maintain authority
 * - Old leader sees higher epoch → steps down
 *
 * <h3>Replication</h3>
 * - Leader: append → apply → return (fire-and-forget)
 * - Background: replicate to peers periodically
 * - WAIT: optionally wait for N replicas to confirm
 *
 * <h3>Risk</h3>
 * If the leader crashes before entries reach a majority, those entries may be lost.
 */
public class AsyncReplicator implements Replicator {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncReplicator.class);

    private final int nodeId;
    private final List<Integer> peerIds;
    private final int majority;

    private volatile long epoch;
    private volatile NodeState state = NodeState.FOLLOWER;
    private volatile int votedFor = -1;
    private volatile int votesReceived = 0;

    private final ReplicationLog log;
    private final StateMachine stateMachine;
    private final ReplicationTransport transport;

    private final PeerState[] peerStates;

    private final ScheduledExecutorService scheduler;
    private final int heartbeatIntervalMs;
    private final int electionTimeoutMs;
    private final Random random = new Random();
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;
    private ScheduledFuture<?> replicationTimer;

    private volatile long commitIndex;
    private volatile long appliedIndex;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private volatile boolean started = false;

    public AsyncReplicator(int nodeId, List<Integer> peerIds, String dataDir,
                           StateMachine stateMachine, ReplicationTransport transport,
                           int heartbeatIntervalMs, int electionTimeoutMs) {
        this.nodeId = nodeId;
        this.peerIds = peerIds;
        this.majority = (peerIds.size() + 1) / 2 + 1;
        this.log = new ReplicationLog(dataDir);
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.electionTimeoutMs = electionTimeoutMs;

        int maxNodeId = nodeId;
        for (int peerId : peerIds) {
            maxNodeId = Math.max(maxNodeId, peerId);
        }
        this.peerStates = new PeerState[maxNodeId + 1];

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "async-repl-" + nodeId);
            t.setDaemon(true);
            return t;
        });
    }

    enum NodeState { FOLLOWER, CANDIDATE, LEADER }

    public boolean isLeader() {
        return state == NodeState.LEADER;
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    public CompletableFuture<ReplicateResult> put(byte[] key, byte[] value) {
        return propose(CommandCodec.encode(CommandCodec.OpType.PUT, key, value));
    }

    @Override
    public CompletableFuture<ReplicateResult> delete(byte[] key) {
        return propose(CommandCodec.encode(CommandCodec.OpType.DELETE, key, null));
    }

    @Override
    public CompletableFuture<ReplicateResult> batchPut(List<WriteOp> ops) {
        return propose(CommandCodec.encodeBatch(ops));
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        return Optional.ofNullable(stateMachine.get(key));
    }

    @Override
    public long getCommitIndex() {
        return commitIndex;
    }

    private CompletableFuture<ReplicateResult> propose(byte[] command) {
        if (state != NodeState.LEADER) {
            return CompletableFuture.completedFuture(new ReplicateResult(-1, false));
        }

        long index = log.lastIndex() + 1;
        ReplicationEntry entry = new ReplicationEntry(index, command);
        log.append(entry);

        stateMachine.apply(command);
        appliedIndex = index;

        LOG.debug("AsyncReplicator node {} proposed entry at index {}", nodeId, index);
        return CompletableFuture.completedFuture(new ReplicateResult(index, true));
    }

    public WaitResult wait(long index, int numReplicas, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            int confirmed = countReplicasAtOrAfter(index);
            if (confirmed >= numReplicas) {
                return new WaitResult(confirmed, true);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new WaitResult(confirmed, false);
            }
        }

        return new WaitResult(countReplicasAtOrAfter(index), false);
    }

    private int countReplicasAtOrAfter(long index) {
        int count = 0;
        for (int peerId : peerIds) {
            if (peerId < peerStates.length && peerStates[peerId] != null) {
                if (peerStates[peerId].matchIndex >= index) {
                    count++;
                }
            }
        }
        return count;
    }

    // ==================== Election ====================

    private void startElection() {
        if (stopped.get()) return;

        synchronized (this) {
            state = NodeState.CANDIDATE;
            epoch++;
            votedFor = nodeId;
            votesReceived = 1;
            persistEpoch();
        }

        LOG.info("AsyncReplicator node {} starting election for epoch {}", nodeId, epoch);

        for (int peerId : peerIds) {
            ReplicationMessage.Heartbeat req = new ReplicationMessage.Heartbeat(nodeId, peerId, epoch);
            transport.sendAsync(req, (ReplicationMessage response) -> {
                if (response instanceof ReplicationMessage.HeartbeatResponse resp) {
                    handleVoteResponse(resp);
                }
            });
        }

        checkElectionWon();
        resetElectionTimer();
    }

    private synchronized void handleVoteResponse(ReplicationMessage.HeartbeatResponse resp) {
        if (state != NodeState.CANDIDATE) return;

        if (resp.epoch() > epoch) {
            becomeFollower(resp.epoch());
            return;
        }

        if (resp.epoch() == epoch && resp.voteGranted()) {
            votesReceived++;
            LOG.debug("Node {} received vote from {} in epoch {} (total={})", nodeId, resp.from(), epoch, votesReceived);
            checkElectionWon();
        }
    }

    private synchronized void checkElectionWon() {
        if (state == NodeState.CANDIDATE && votesReceived >= majority) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        state = NodeState.LEADER;
        LOG.info("AsyncReplicator node {} became leader for epoch {}", nodeId, epoch);

        long lastIdx = log.lastIndex();
        for (int i = 0; i < peerStates.length; i++) {
            peerStates[i] = new PeerState(lastIdx + 1, 0);
        }

        cancelElectionTimer();
        startHeartbeats();
        startReplication();
    }

    private synchronized void becomeFollower(long newEpoch) {
        if (newEpoch > epoch) {
            epoch = newEpoch;
            votedFor = -1;
            persistEpoch();
        }
        state = NodeState.FOLLOWER;

        cancelHeartbeats();
        cancelReplication();
        resetElectionTimer();

        LOG.debug("Node {} became follower for epoch {}", nodeId, epoch);
    }

    // ==================== Handle messages ====================

    public ReplicationMessage.HeartbeatResponse handleHeartbeat(ReplicationMessage.Heartbeat req) {
        synchronized (this) {
            if (req.epoch() > epoch) {
                becomeFollower(req.epoch());
                votedFor = req.from();
                return new ReplicationMessage.HeartbeatResponse(nodeId, req.from(), epoch, true);
            }

            if (req.epoch() == epoch) {
                if (state == NodeState.LEADER) {
                    if (req.from() < nodeId) {
                        becomeFollower(req.epoch());
                        votedFor = req.from();
                        return new ReplicationMessage.HeartbeatResponse(nodeId, req.from(), epoch, true);
                    }
                    return new ReplicationMessage.HeartbeatResponse(nodeId, req.from(), epoch, false);
                }

                if (state == NodeState.FOLLOWER) {
                    votedFor = req.from();
                    resetElectionTimer();
                    return new ReplicationMessage.HeartbeatResponse(nodeId, req.from(), epoch, true);
                }

                if (state == NodeState.CANDIDATE) {
                    if (votedFor == nodeId) {
                        return new ReplicationMessage.HeartbeatResponse(nodeId, req.from(), epoch, false);
                    }
                    return new ReplicationMessage.HeartbeatResponse(nodeId, req.from(), epoch, votedFor == req.from());
                }
            }

            return new ReplicationMessage.HeartbeatResponse(nodeId, req.from(), epoch, false);
        }
    }

    public ReplicationMessage.ReplicateEntriesResponse handleReplicateEntries(
            ReplicationMessage.ReplicateEntries req) {

        if (req.prevIndex() > 0 && req.prevIndex() >= log.size()) {
            return new ReplicationMessage.ReplicateEntriesResponse(
                    nodeId, req.from(), false, log.lastIndex());
        }

        List<ReplicationEntry> newEntries = (req.entries() != null) ? Arrays.asList(req.entries()) : List.of();
        boolean success = log.append(req.prevIndex(), newEntries);

        if (success) {
            for (ReplicationEntry entry : newEntries) {
                stateMachine.apply(entry.data());
                appliedIndex = entry.index();
            }
        }

        return new ReplicationMessage.ReplicateEntriesResponse(
                nodeId, req.from(), success, log.lastIndex());
    }

    // ==================== Background replication ====================

    private void startReplication() {
        replicationTimer = scheduler.scheduleAtFixedRate(() -> {
            if (state == NodeState.LEADER && !stopped.get()) {
                replicateToFollowers();
            }
        }, 0, 200, TimeUnit.MILLISECONDS);
    }

    private void cancelReplication() {
        if (replicationTimer != null) {
            replicationTimer.cancel(false);
        }
    }

    private void replicateToFollowers() {
        for (int peerId : peerIds) {
            if (peerId < peerStates.length && peerStates[peerId] != null) {
                PeerState ps = peerStates[peerId];
                if (ps.nextIndex <= log.lastIndex()) {
                    sendEntriesToPeer(peerId, ps);
                }
            }
        }
    }

    private void sendEntriesToPeer(int peerId, PeerState ps) {
        long nextIdx = ps.nextIndex;
        long prevIndex = nextIdx - 1;

        List<ReplicationEntry> entriesToSend = log.slice(nextIdx, Math.min(nextIdx + 64, log.size()));
        ReplicationEntry[] entriesArray = entriesToSend.toArray(new ReplicationEntry[0]);

        ReplicationMessage.ReplicateEntries msg = new ReplicationMessage.ReplicateEntries(
                nodeId, peerId, prevIndex, entriesArray);

        transport.sendAsync(msg, (ReplicationMessage response) -> {
            if (response instanceof ReplicationMessage.ReplicateEntriesResponse resp) {
                handleReplicateEntriesResponse(resp, peerId);
            }
        });
    }

    private void handleReplicateEntriesResponse(ReplicationMessage.ReplicateEntriesResponse resp, int peerId) {
        if (resp.from() != peerId) return;
        if (peerId >= peerStates.length || peerStates[peerId] == null) return;

        if (resp.success()) {
            peerStates[peerId].matchIndex = resp.matchIndex();
            peerStates[peerId].nextIndex = resp.matchIndex() + 1;
            maybeAdvanceCommit();
        } else {
            if (peerStates[peerId].nextIndex > 1) {
                peerStates[peerId].nextIndex--;
            }
        }
    }

    private void maybeAdvanceCommit() {
        for (long n = commitIndex + 1; n <= appliedIndex; n++) {
            boolean allConfirmed = true;
            for (int peerId : peerIds) {
                if (peerId < peerStates.length && peerStates[peerId] != null) {
                    if (peerStates[peerId].matchIndex < n) {
                        allConfirmed = false;
                        break;
                    }
                }
            }
            if (allConfirmed) {
                commitIndex = n;
            } else {
                break;
            }
        }
    }

    // ==================== Heartbeats ====================

    private void startHeartbeats() {
        heartbeatTimer = scheduler.scheduleAtFixedRate(() -> {
            if (state == NodeState.LEADER && !stopped.get()) {
                sendHeartbeats();
            }
        }, 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void cancelHeartbeats() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
        }
    }

    private void sendHeartbeats() {
        for (int peerId : peerIds) {
            ReplicationMessage.Heartbeat hb = new ReplicationMessage.Heartbeat(nodeId, peerId, epoch);
            transport.sendAsync(hb, (ReplicationMessage response) -> {
                if (response instanceof ReplicationMessage.HeartbeatResponse resp) {
                    if (resp.epoch() > epoch) {
                        synchronized (AsyncReplicator.this) {
                            if (resp.epoch() > epoch) {
                                becomeFollower(resp.epoch());
                            }
                        }
                    }
                }
            });
        }
    }

    // ==================== Election timer ====================

    private void resetElectionTimer() {
        cancelElectionTimer();
        int timeout = electionTimeoutMs + random.nextInt(electionTimeoutMs);
        electionTimer = scheduler.schedule(() -> {
            if (state != NodeState.LEADER && !stopped.get()) {
                startElection();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private void cancelElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
    }

    // ==================== Epoch persistence ====================

    private void persistEpoch() {
        try {
            Path stateFile = Paths.get(log.getLogDir(), "repl.state");
            Files.createDirectories(stateFile.getParent());
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(stateFile)))) {
                dos.writeLong(epoch);
            }
        } catch (IOException e) {
            LOG.error("Failed to persist epoch", e);
        }
    }

    private long recoverEpoch() {
        try {
            Path stateFile = Paths.get(log.getLogDir(), "repl.state");
            if (!Files.exists(stateFile)) return 0;
            try (DataInputStream dis = new DataInputStream(new BufferedInputStream(
                    Files.newInputStream(stateFile)))) {
                return dis.readLong();
            }
        } catch (IOException e) {
            LOG.error("Failed to recover epoch", e);
            return 0;
        }
    }

    // ==================== Lifecycle ====================

    @Override
    public void start() {
        if (started) return;

        try {
            log.init();
            appliedIndex = log.lastIndex();
        } catch (IOException e) {
            LOG.error("Failed to init async replication log", e);
        }

        epoch = recoverEpoch();

        transport.register(nodeId, this);
        resetElectionTimer();

        started = true;
        LOG.info("AsyncReplicator node {} started (epoch={})", nodeId, epoch);
    }

    @Override
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            cancelElectionTimer();
            cancelHeartbeats();
            cancelReplication();
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("AsyncReplicator node {} stopped", nodeId);
        }
    }

    static class PeerState {
        volatile long nextIndex;
        volatile long matchIndex;

        PeerState(long nextIndex, long matchIndex) {
            this.nextIndex = nextIndex;
            this.matchIndex = matchIndex;
        }
    }
}
