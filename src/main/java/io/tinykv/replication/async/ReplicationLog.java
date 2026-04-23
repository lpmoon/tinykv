package io.tinykv.replication.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Persistent log for async replication.
 */
public class ReplicationLog {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLog.class);

    private final List<ReplicationEntry> entries;
    private final String logDir;

    public ReplicationLog(String logDir) {
        this.logDir = logDir;
        this.entries = new ArrayList<>();
        this.entries.add(new ReplicationEntry(0, new byte[0]));
    }

    public String getLogDir() {
        return logDir;
    }

    public void init() throws IOException {
        Files.createDirectories(Paths.get(logDir));
        recover();
    }

    public int size() {
        return entries.size();
    }

    public long lastIndex() {
        return entries.size() - 1;
    }

    public ReplicationEntry get(long index) {
        if (index <= 0 || index >= entries.size()) return null;
        return entries.get((int) index);
    }

    public List<ReplicationEntry> slice(long startIndex, long endIndex) {
        List<ReplicationEntry> result = new ArrayList<>();
        for (long i = startIndex; i < endIndex && i < entries.size(); i++) {
            if (i > 0) {
                result.add(entries.get((int) i));
            }
        }
        return result;
    }

    public long append(ReplicationEntry entry) {
        entries.add(entry);
        persistEntry(entry);
        return entry.index();
    }

    public boolean append(long prevIndex, List<ReplicationEntry> newEntries) {
        if (prevIndex >= entries.size()) {
            return false;
        }

        for (int i = 0; i < newEntries.size(); i++) {
            long logIndex = prevIndex + 1 + i;
            ReplicationEntry newEntry = newEntries.get(i);

            if (logIndex < entries.size()) {
                // Already have this entry — skip
            } else {
                entries.add(newEntry);
                persistEntry(newEntry);
            }
        }

        return true;
    }

    private void persistEntry(ReplicationEntry entry) {
        try {
            Path logFile = Paths.get(logDir, "repl.log");
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(logFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)))) {
                dos.writeLong(entry.index());
                dos.writeInt(entry.data().length);
                dos.write(entry.data());
            }
        } catch (IOException e) {
            LOG.error("Failed to persist replication log entry", e);
        }
    }

    private void recover() throws IOException {
        Path logFile = Paths.get(logDir, "repl.log");
        if (!Files.exists(logFile)) return;

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(logFile)))) {
            while (dis.available() > 0) {
                try {
                    long index = dis.readLong();
                    int dataLen = dis.readInt();
                    byte[] data = new byte[dataLen];
                    dis.readFully(data);
                    entries.add(new ReplicationEntry(index, data));
                } catch (EOFException e) {
                    break;
                }
            }
        }

        LOG.info("Recovered replication log: {} entries", entries.size() - 1);
    }
}
