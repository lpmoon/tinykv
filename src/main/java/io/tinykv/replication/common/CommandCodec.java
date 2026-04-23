package io.tinykv.replication.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes and decodes KV commands (PUT, DELETE, BATCH) into byte arrays.
 * Shared between SyncReplicator and AsyncReplicator.
 */
public class CommandCodec {

    public static byte[] encode(OpType type, byte[] key, byte[] value) {
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

    public static byte[] encodeBatch(List<WriteOp> ops) {
        int totalSize = 1 + 4; // type + count
        for (WriteOp op : ops) {
            totalSize += 1 + 4 + op.key().length + 4 + (op.value() != null ? op.value().length : 0);
        }

        byte[] buf = new byte[totalSize];
        int offset = 0;
        buf[offset++] = (byte) OpType.BATCH.ordinal();
        buf[offset] = (byte) (ops.size() >> 24); buf[offset+1] = (byte) (ops.size() >> 16);
        buf[offset+2] = (byte) (ops.size() >> 8); buf[offset+3] = (byte) ops.size();
        offset += 4;

        for (WriteOp op : ops) {
            buf[offset++] = (byte) op.type().ordinal();
            int keyLen = op.key().length;
            buf[offset] = (byte) (keyLen >> 24); buf[offset+1] = (byte) (keyLen >> 16);
            buf[offset+2] = (byte) (keyLen >> 8); buf[offset+3] = (byte) keyLen;
            offset += 4;
            System.arraycopy(op.key(), 0, buf, offset, keyLen);
            offset += keyLen;
            int valueLen = (op.value() != null) ? op.value().length : 0;
            buf[offset] = (byte) (valueLen >> 24); buf[offset+1] = (byte) (valueLen >> 16);
            buf[offset+2] = (byte) (valueLen >> 8); buf[offset+3] = (byte) valueLen;
            offset += 4;
            if (op.value() != null && valueLen > 0) {
                System.arraycopy(op.value(), 0, buf, offset, valueLen);
                offset += valueLen;
            }
        }
        return buf;
    }

    public static Command decode(byte[] data) {
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

    public enum OpType { PUT, DELETE, BATCH }

    public static class Command {
        public final OpType type;
        public final byte[] key;
        public final byte[] value;
        public final List<WriteOp> ops;

        public Command(OpType type, byte[] key, byte[] value, List<WriteOp> ops) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.ops = ops;
        }
    }
}
