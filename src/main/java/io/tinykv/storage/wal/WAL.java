package io.tinykv.storage.wal;

import io.tinykv.common.Record.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Write-Ahead Log for crash recovery.
 * Each write operation is appended to the WAL before applying to MemTable.
 *
 * On-disk entry format:
 *   [4: totalLen] [4: crc32] [1: type] [4: keyLen] [key bytes] [4: valueLen] [value bytes]
 *   totalLen = 1 + 4 + keyLen + 4 + valueLen (everything after crc32)
 */
public class WAL implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(WAL.class);

    private final Path walDir;
    private DataOutputStream writer;
    private Path currentWalPath;
    private final AtomicLong sequence = new AtomicLong(0);

    public WAL(String walDir) throws IOException {
        this.walDir = Paths.get(walDir);
        Files.createDirectories(this.walDir);
        openNewFile();
    }

    private void openNewFile() throws IOException {
        long seq = sequence.get();
        currentWalPath = walDir.resolve(String.format("wal-%020d.log", seq));
        writer = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(currentWalPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)));
        LOG.info("Opened WAL file: {}", currentWalPath);
    }

    /**
     * Append an entry to the WAL. Must be called before applying to MemTable.
     */
    public synchronized void append(WALEntry entry) throws IOException {
        byte[] record = serializeEntry(entry);
        writer.writeInt(record.length);        // totalLen
        writer.write(record);                  // crc32 + payload
        writer.flush();
    }

    /**
     * Append a batch of entries atomically. Syncs once at the end.
     */
    public synchronized void appendBatch(List<WALEntry> entries) throws IOException {
        for (WALEntry entry : entries) {
            byte[] record = serializeEntry(entry);
            writer.writeInt(record.length);
            writer.write(record);
        }
        writer.flush();
    }

    /**
     * Force sync to disk (fsync).
     */
    public synchronized void sync() throws IOException {
        if (writer != null) {
            writer.flush();
        }
    }

    /**
     * Rotate WAL: close current file and open a new one.
     * Called after MemTable is flushed to SSTable.
     */
    public synchronized void rotate() throws IOException {
        close();
        sequence.incrementAndGet();
        openNewFile();
    }

    /**
     * Recover all entries from WAL files on disk.
     * Returns entries in order from oldest to newest.
     */
    public List<WALEntry> recover() throws IOException {
        List<WALEntry> allEntries = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(walDir, "wal-*.log")) {
            List<Path> walFiles = new ArrayList<>();
            for (Path p : stream) {
                walFiles.add(p);
            }
            walFiles.sort(Comparator.comparing(Path::toString));

            for (Path walFile : walFiles) {
                LOG.info("Recovering WAL file: {}", walFile);
                List<WALEntry> entries = readWalFile(walFile);
                allEntries.addAll(entries);
            }
        }

        LOG.info("Recovered {} entries from WAL", allEntries.size());
        return allEntries;
    }

    private List<WALEntry> readWalFile(Path path) {
        List<WALEntry> entries = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            while (dis.available() > 4) {
                int totalLen;
                try {
                    totalLen = dis.readInt();
                } catch (EOFException e) {
                    break;
                }
                if (totalLen <= 0 || totalLen > 64 * 1024 * 1024) {
                    LOG.warn("Invalid record length {} in {}, stopping", totalLen, path);
                    break;
                }
                byte[] record = new byte[totalLen];
                try {
                    dis.readFully(record);
                } catch (EOFException e) {
                    LOG.warn("Truncated record in {}, stopping", path);
                    break;
                }

                WALEntry entry = deserializeEntry(record);
                if (entry != null) {
                    entries.add(entry);
                } else {
                    LOG.warn("CRC mismatch in {}, stopping at this point", path);
                    break;
                }
            }
        } catch (IOException e) {
            LOG.warn("Error reading WAL file {}: {}", path, e.getMessage());
        }
        return entries;
    }

    private byte[] serializeEntry(WALEntry entry) {
        int keyLen = entry.key().length;
        int valueLen = (entry.value() != null) ? entry.value().length : 0;
        // Layout: CRC(4) + type(1) + keyLen(4) + key(keyLen) + valueLen(4) + value(valueLen)
        int payloadLen = 1 + 4 + keyLen + 4 + valueLen;
        ByteBuffer buf = ByteBuffer.allocate(4 + payloadLen);

        // Write fields at fixed positions: CRC(0-3), type(4), keyLen(5-8), key(9..), valueLen(...), value(...)
        int pos = 4; // Start after CRC slot
        buf.position(pos);
        buf.put((byte) entry.type().ordinal());  // pos = 5
        buf.putInt(keyLen);  // pos = 9
        buf.put(entry.key());  // pos = 9 + keyLen
        buf.putInt(valueLen);  // pos = 9 + keyLen + 4
        if (entry.value() != null) {
            buf.put(entry.value());  // pos = 9 + keyLen + 4 + valueLen
        }

        // Compute CRC32 over the payload (from type onwards, skipping CRC slot)
        byte[] payload = buf.array();
        CRC32 crc = new CRC32();
        crc.update(payload, 4, payloadLen);
        int crcValue = (int) crc.getValue();

        // Write CRC at the beginning
        buf.putInt(0, crcValue);
        return buf.array();
    }

    private WALEntry deserializeEntry(byte[] record) {
        ByteBuffer buf = ByteBuffer.wrap(record);

        int storedCrc = buf.getInt();  // 0-3
        byte typeByte = buf.get();     // 4
        int keyLen = buf.getInt();    // 5-8
        if (keyLen < 0 || keyLen > record.length) return null;
        byte[] key = new byte[keyLen];
        buf.get(key);                 // 9 .. 9+keyLen-1
        int valueLen = buf.getInt();   // 9+keyLen .. 9+keyLen+3
        if (valueLen < 0 || valueLen > record.length) return null;
        byte[] value = new byte[valueLen];
        if (valueLen > 0) {
            buf.get(value);           // 9+keyLen+4 .. 9+keyLen+4+valueLen-1
        }

        // Verify CRC
        CRC32 crc = new CRC32();
        crc.update(record, 4, record.length - 4);
        int computedCrc = (int) crc.getValue();

        if (storedCrc != computedCrc) {
            LOG.warn("CRC mismatch: stored={}, computed={}", storedCrc, computedCrc);
            return null;
        }

        RecordType type = (typeByte == 0) ? RecordType.PUT : RecordType.DELETE;
        return new WALEntry(type, key, value);
    }

    @Override
    public synchronized void close() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
            writer = null;
        }
    }
}
