package io.tinykv.replication.sync;

import io.tinykv.raft.*;
import io.tinykv.replication.common.CommandCodec;
import io.tinykv.replication.common.ReplicateResult;
import io.tinykv.replication.common.Replicator;
import io.tinykv.replication.common.WriteOp;
import io.tinykv.storage.MVCCStorage;
import io.tinykv.common.Config;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Synchronous replication via Raft.
 * All writes go through the Raft leader and must be confirmed by a majority
 * before being applied to the state machine.
 */
public class SyncReplicator implements Replicator {

    private static final Logger LOG = LoggerFactory.getLogger(SyncReplicator.class);

    private final RaftNode raftNode;
    private final MVCCStorage mvccStorage;
    private final Config config;

    public SyncReplicator(RaftNode raftNode, MVCCStorage mvccStorage, Config config) {
        this.raftNode = raftNode;
        this.mvccStorage = mvccStorage;
        this.config = config;
    }

    @Override
    public CompletableFuture<ReplicateResult> put(byte[] key, byte[] value) {
        ensureLeader();
        byte[] command = CommandCodec.encode(CommandCodec.OpType.PUT, key, value);
        return proposeAsync(command);
    }

    @Override
    public CompletableFuture<ReplicateResult> delete(byte[] key) {
        ensureLeader();
        byte[] command = CommandCodec.encode(CommandCodec.OpType.DELETE, key, null);
        return proposeAsync(command);
    }

    @Override
    public CompletableFuture<ReplicateResult> batchPut(List<WriteOp> ops) {
        ensureLeader();
        byte[] command = CommandCodec.encodeBatch(ops);
        return proposeAsync(command);
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        if (!raftNode.isLeader()) {
            throw new NotLeaderException(raftNode.getLeaderId());
        }
        try {
            return mvccStorage.get(key);
        } catch (IOException e) {
            throw new RuntimeException("MVCC get failed", e);
        }
    }

    @Override
    public long getCommitIndex() {
        return raftNode.getCommitIndex();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    private CompletableFuture<ReplicateResult> proposeAsync(byte[] command) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                raftNode.proposeAndWait(command, 5000);
                return new ReplicateResult(raftNode.getCommitIndex(), true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new ReplicateResult(-1, false);
            } catch (TimeoutException e) {
                return new ReplicateResult(-1, false);
            }
        });
    }

    private void ensureLeader() {
        if (!raftNode.isLeader()) {
            throw new NotLeaderException(raftNode.getLeaderId());
        }
    }

    public RaftNode getRaftNode() {
        return raftNode;
    }

    public static class LSMTreeStateMachine implements StateMachine {

        private final MVCCStorage mvccStorage;

        public LSMTreeStateMachine(MVCCStorage mvccStorage) {
            this.mvccStorage = mvccStorage;
        }

        @Override
        public byte[] apply(byte[] command) {
            CommandCodec.Command cmd = CommandCodec.decode(command);
            if (cmd == null) return null;

            switch (cmd.type) {
                case PUT -> mvccStorage.put(cmd.key, cmd.value);
                case DELETE -> mvccStorage.delete(cmd.key);
                case BATCH -> {
                    // Txn.commit() pre-encodes MVCC keys, so write directly
                    mvccStorage.writeTxnBatch(cmd.ops);
                }
            }
            return null;
        }

        @Override
        public byte[] get(byte[] key) {
            try {
                return mvccStorage.get(key).orElse(null);
            } catch (IOException e) {
                throw new RuntimeException("MVCC get failed", e);
            }
        }

        @Override
        public byte[] snapshot() {
            return new byte[0];
        }

        @Override
        public void restore(byte[] snapshot) {
        }
    }

    public static class NotLeaderException extends RuntimeException {
        private final int leaderId;

        public NotLeaderException(int leaderId) {
            super("Not the leader. Current leader: " + (leaderId >= 0 ? leaderId : "unknown"));
            this.leaderId = leaderId;
        }

        public int getLeaderId() { return leaderId; }
    }
}
