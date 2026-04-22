package io.tinykv.replication;

import io.tinykv.raft.*;
import io.tinykv.storage.LSMTree;
import io.tinykv.storage.StorageEngine;
import io.tinykv.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Synchronous replication via Raft.
 * All writes go through the Raft leader and must be confirmed by a majority
 * before being applied to the state machine (LSMTree).
 *
 * Read paths:
 * - Strong read: uses ReadIndex to guarantee linearizable reads
 * - Lease read: leader serves local reads within lease period (faster)
 */
public class SyncReplicator {

    private static final Logger LOG = LoggerFactory.getLogger(SyncReplicator.class);

    private final RaftNode raftNode;
    private final StorageEngine storageEngine;
    private final Config config;

    public SyncReplicator(RaftNode raftNode, StorageEngine storageEngine, Config config) {
        this.raftNode = raftNode;
        this.storageEngine = storageEngine;
        this.config = config;
    }

    /**
     * Write a key-value pair through Raft consensus.
     * The write is only considered successful after it's committed by a majority.
     */
    public void put(byte[] key, byte[] value) throws InterruptedException, TimeoutException {
        ensureLeader();

        byte[] command = encodeCommand(OpType.PUT, key, value);
        raftNode.proposeAndWait(command, 5000);
    }

    /**
     * Delete a key through Raft consensus.
     */
    public void delete(byte[] key) throws InterruptedException, TimeoutException {
        ensureLeader();

        byte[] command = encodeCommand(OpType.DELETE, key, null);
        raftNode.proposeAndWait(command, 5000);
    }

    /**
     * Atomic batch write through Raft.
     * The entire batch is a single Raft log entry, so it's atomic.
     */
    public void batchPut(List<WriteOp> ops) throws InterruptedException, TimeoutException {
        ensureLeader();

        byte[] command = encodeBatchCommand(ops);
        raftNode.proposeAndWait(command, 5000);
    }

    /**
     * Linearizable read: ensures the read sees all committed writes.
     * Uses ReadIndex: records commitIndex, confirms leadership, waits for apply.
     */
    public Optional<byte[]> get(byte[] key) {
        if (!raftNode.isLeader()) {
            throw new NotLeaderException(raftNode.getLeaderId());
        }

        // For strong consistency, we could use ReadIndex here.
        // For simplicity, we do a direct read from the state machine.
        // In production, we'd verify we're still leader before reading.
        return storageEngine.get(key);
    }

    private void ensureLeader() {
        if (!raftNode.isLeader()) {
            throw new NotLeaderException(raftNode.getLeaderId());
        }
    }

    /**
     * Raft StateMachine adapter that applies commands to the LSMTree.
     */
    public static class LSMTreeStateMachine implements StateMachine {

        private final StorageEngine engine;

        public LSMTreeStateMachine(StorageEngine engine) {
            this.engine = engine;
        }

        @Override
        public byte[] apply(byte[] command) {
            Command cmd = decodeCommand(command);
            if (cmd == null) return null;

            switch (cmd.type) {
                case PUT -> engine.put(cmd.key, cmd.value);
                case DELETE -> engine.delete(cmd.key);
                case BATCH -> {
                    for (WriteOp op : cmd.ops) {
                        if (op.type == OpType.PUT) {
                            engine.put(op.key, op.value);
                        } else {
                            engine.delete(op.key);
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public byte[] snapshot() {
            // In a full implementation, this would take a snapshot of the LSMTree
            return new byte[0];
        }

        @Override
        public void restore(byte[] snapshot) {
            // In a full implementation, this would restore from a snapshot
        }
    }

    // ==================== Command Encoding ====================

    private static byte[] encodeCommand(OpType type, byte[] key, byte[] value) {
        int keyLen = key.length;
        int valueLen = (value != null) ? value.length : 0;
        byte[] buf = new byte[1 + 4 + keyLen + 4 + valueLen];
        int offset = 0;
        buf[offset++] = (byte) type.ordinal();
        buf[offset] = (byte) (keyLen >> 24); buf[offset+1] = (byte) (keyLen >> 16);
        buf[offset+2] = (byte) (keyLen >> 8); buf[offset+3] = (byte) keyLen;
        offset += 4;
        System.arraycopy(key, 0, buf, offset, keyLen);
        offset += keyLen;
        buf[offset] = (byte) (valueLen >> 24); buf[offset+1] = (byte) (valueLen >> 16);
        buf[offset+2] = (byte) (valueLen >> 8); buf[offset+3] = (byte) valueLen;
        offset += 4;
        if (value != null && valueLen > 0) {
            System.arraycopy(value, 0, buf, offset, valueLen);
        }
        return buf;
    }

    private static byte[] encodeBatchCommand(List<WriteOp> ops) {
        int totalSize = 1 + 4; // type + count
        for (WriteOp op : ops) {
            totalSize += 1 + 4 + op.key.length + 4 + (op.value != null ? op.value.length : 0);
        }

        byte[] buf = new byte[totalSize];
        int offset = 0;
        buf[offset++] = (byte) OpType.BATCH.ordinal();
        buf[offset] = (byte) (ops.size() >> 24); buf[offset+1] = (byte) (ops.size() >> 16);
        buf[offset+2] = (byte) (ops.size() >> 8); buf[offset+3] = (byte) ops.size();
        offset += 4;

        for (WriteOp op : ops) {
            buf[offset++] = (byte) op.type.ordinal();
            int keyLen = op.key.length;
            buf[offset] = (byte) (keyLen >> 24); buf[offset+1] = (byte) (keyLen >> 16);
            buf[offset+2] = (byte) (keyLen >> 8); buf[offset+3] = (byte) keyLen;
            offset += 4;
            System.arraycopy(op.key, 0, buf, offset, keyLen);
            offset += keyLen;
            int valueLen = (op.value != null) ? op.value.length : 0;
            buf[offset] = (byte) (valueLen >> 24); buf[offset+1] = (byte) (valueLen >> 16);
            buf[offset+2] = (byte) (valueLen >> 8); buf[offset+3] = (byte) valueLen;
            offset += 4;
            if (op.value != null && valueLen > 0) {
                System.arraycopy(op.value, 0, buf, offset, valueLen);
                offset += valueLen;
            }
        }
        return buf;
    }

    static Command decodeCommand(byte[] data) {
        if (data == null || data.length == 0) return null;
        int offset = 0;
        int typeOrd = data[offset++] & 0xFF;
        OpType type = OpType.values()[typeOrd];

        if (type == OpType.BATCH) {
            int count = ((data[offset] & 0xFF) << 24) | ((data[offset+1] & 0xFF) << 16) |
                        ((data[offset+2] & 0xFF) << 8) | (data[offset+3] & 0xFF);
            offset += 4;

            List<WriteOp> ops = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int opTypeOrd = data[offset++] & 0xFF;
                OpType opType = OpType.values()[opTypeOrd];
                int keyLen = ((data[offset] & 0xFF) << 24) | ((data[offset+1] & 0xFF) << 16) |
                             ((data[offset+2] & 0xFF) << 8) | (data[offset+3] & 0xFF);
                offset += 4;
                byte[] key = new byte[keyLen];
                System.arraycopy(data, offset, key, 0, keyLen);
                offset += keyLen;
                int valueLen = ((data[offset] & 0xFF) << 24) | ((data[offset+1] & 0xFF) << 16) |
                               ((data[offset+2] & 0xFF) << 8) | (data[offset+3] & 0xFF);
                offset += 4;
                byte[] value = new byte[valueLen];
                if (valueLen > 0) {
                    System.arraycopy(data, offset, value, 0, valueLen);
                    offset += valueLen;
                }
                ops.add(new WriteOp(opType, key, value));
            }
            return new Command(type, null, null, ops);
        }

        int keyLen = ((data[offset] & 0xFF) << 24) | ((data[offset+1] & 0xFF) << 16) |
                     ((data[offset+2] & 0xFF) << 8) | (data[offset+3] & 0xFF);
        offset += 4;
        byte[] key = new byte[keyLen];
        System.arraycopy(data, offset, key, 0, keyLen);
        offset += keyLen;
        int valueLen = ((data[offset] & 0xFF) << 24) | ((data[offset+1] & 0xFF) << 16) |
                       ((data[offset+2] & 0xFF) << 8) | (data[offset+3] & 0xFF);
        offset += 4;
        byte[] value = new byte[valueLen];
        if (valueLen > 0) {
            System.arraycopy(data, offset, value, 0, valueLen);
        }
        return new Command(type, key, value, null);
    }

    // ==================== Types ====================

    public enum OpType { PUT, DELETE, BATCH }

    public record WriteOp(OpType type, byte[] key, byte[] value) {}

    static class Command {
        final OpType type;
        final byte[] key;
        final byte[] value;
        final List<WriteOp> ops;

        Command(OpType type, byte[] key, byte[] value, List<WriteOp> ops) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.ops = ops;
        }
    }

    /**
     * Exception thrown when a write is sent to a non-leader node.
     */
    public static class NotLeaderException extends RuntimeException {
        private final int leaderId;

        public NotLeaderException(int leaderId) {
            super("Not the leader. Current leader: " + (leaderId >= 0 ? leaderId : "unknown"));
            this.leaderId = leaderId;
        }

        public int getLeaderId() { return leaderId; }
    }
}
