package io.tinykv.raft;

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
 * Persistent Raft log storage.
 * Log entries are stored on disk for crash recovery.
 */
public class RaftLog {

    private static final Logger LOG = LoggerFactory.getLogger(RaftLog.class);

    private final List<LogEntry> entries; // index 0 is a sentinel (index 1 is the first real entry)
    private volatile long commitIndex;
    private volatile long appliedIndex;

    private final String logDir;

    public RaftLog(String logDir) {
        this.logDir = logDir;
        this.entries = new ArrayList<>();
        this.entries.add(new LogEntry(0, 0, new byte[0])); // sentinel
        this.commitIndex = 0;
        this.appliedIndex = 0;
    }

    public void init() throws IOException {
        Files.createDirectories(Paths.get(logDir));
        recover();
    }

    /**
     * Get the number of entries (including sentinel).
     */
    public int size() {
        return entries.size();
    }

    /**
     * Get the last log index.
     */
    public long lastLogIndex() {
        return entries.size() - 1;
    }

    /**
     * Get the term of the last log entry.
     */
    public long lastLogTerm() {
        if (entries.size() <= 1) return 0;
        return entries.get(entries.size() - 1).term();
    }

    /**
     * Get term at a given index.
     */
    public long termAt(long index) {
        if (index <= 0 || index >= entries.size()) return 0;
        return entries.get((int) index).term();
    }

    /**
     * Get entry at a given index.
     */
    public LogEntry get(long index) {
        if (index <= 0 || index >= entries.size()) return null;
        return entries.get((int) index);
    }

    /**
     * Get entries in [startIndex, endIndex).
     */
    public List<LogEntry> slice(long startIndex, long endIndex) {
        List<LogEntry> result = new ArrayList<>();
        for (long i = startIndex; i < endIndex && i < entries.size(); i++) {
            if (i > 0) {
                result.add(entries.get((int) i));
            }
        }
        return result;
    }

    /**
     * Append entries to the log (after truncating conflicting entries).
     * Returns false if prevLogIndex/prevLogTerm don't match.
     */
    public boolean append(long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {
        // Check consistency
        if (prevLogIndex >= entries.size()) {
            return false;
        }
        if (prevLogIndex > 0 && entries.get((int) prevLogIndex).term() != prevLogTerm) {
            // Conflict: truncate from this point
            truncateFrom(prevLogIndex + 1);
            return false; // Will retry in next AppendEntries
        }

        // Append new entries, handling conflicts
        for (int i = 0; i < newEntries.size(); i++) {
            long logIndex = prevLogIndex + 1 + i;
            LogEntry newEntry = newEntries.get(i);

            if (logIndex < entries.size()) {
                // Check for conflict
                LogEntry existing = entries.get((int) logIndex);
                if (existing.term() != newEntry.term()) {
                    // Conflict: truncate from here
                    truncateFrom(logIndex);
                    entries.add(newEntry);
                    persistEntry(newEntry);
                }
                // else: same term, already have this entry, skip
            } else {
                entries.add(newEntry);
                persistEntry(newEntry);
            }
        }

        return true;
    }

    /**
     * Append a single entry from the leader (client request).
     */
    public long append(LogEntry entry) {
        entries.add(entry);
        persistEntry(entry);
        return entry.index();
    }

    /**
     * Truncate all entries from index onwards.
     */
    private void truncateFrom(long index) {
        while (entries.size() > index) {
            entries.remove(entries.size() - 1);
        }
        // In production, we'd also truncate the on-disk file
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = Math.max(this.commitIndex, commitIndex);
    }

    public long getAppliedIndex() {
        return appliedIndex;
    }

    public void setAppliedIndex(long appliedIndex) {
        this.appliedIndex = appliedIndex;
    }

    /**
     * Persist a log entry to disk.
     */
    private void persistEntry(LogEntry entry) {
        try {
            Path logFile = Paths.get(logDir, "raft.log");
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(logFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)))) {
                dos.writeLong(entry.term());
                dos.writeLong(entry.index());
                dos.writeInt(entry.data().length);
                dos.write(entry.data());
            }
        } catch (IOException e) {
            LOG.error("Failed to persist Raft log entry", e);
        }
    }

    /**
     * Recover Raft log from disk.
     */
    private void recover() throws IOException {
        Path logFile = Paths.get(logDir, "raft.log");
        if (!Files.exists(logFile)) return;

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(logFile)))) {
            while (dis.available() > 0) {
                try {
                    long term = dis.readLong();
                    long index = dis.readLong();
                    int dataLen = dis.readInt();
                    byte[] data = new byte[dataLen];
                    dis.readFully(data);
                    entries.add(new LogEntry(term, index, data));
                } catch (EOFException e) {
                    break;
                }
            }
        }

        LOG.info("Recovered Raft log: {} entries", entries.size() - 1);
    }

    /**
     * Persist hard state (currentTerm, votedFor, commitIndex, appliedIndex).
     *
     * <p>参考 etcd 的做法：每当 commitIndex 或 appliedIndex 变化时都持久化，
     * 这样重启后能尽量恢复到最新的进度，避免重复 apply 或遗漏 apply。
     * appliedIndex 持久化后可作为恢复基准：已 apply 的日志一定已 commit，
     * 重启时 commitIndex 至少可以恢复到 appliedIndex。
     */
    public void persistHardState(long currentTerm, int votedFor, long commitIndex) {
        try {
            Path stateFile = Paths.get(logDir, "raft.state");
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(stateFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)))) {
                dos.writeLong(currentTerm);
                dos.writeInt(votedFor);
                dos.writeLong(commitIndex);
                dos.writeLong(appliedIndex); // 额外持久化 appliedIndex，用于重启恢复
            }
        } catch (IOException e) {
            LOG.error("Failed to persist hard state", e);
        }
    }

    /**
     * Recover hard state from disk.
     * Returns [currentTerm, votedFor, commitIndex, appliedIndex].
     */
    public long[] recoverHardState() throws IOException {
        Path stateFile = Paths.get(logDir, "raft.state");
        if (!Files.exists(stateFile)) return new long[]{0, -1, 0, 0};

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(stateFile)))) {
            long currentTerm = dis.readLong();
            int votedFor = dis.readInt();
            long commitIndex = dis.readLong();
            // 兼容旧格式（没有 appliedIndex 字段）
            long appliedIndex = 0;
            try {
                appliedIndex = dis.readLong();
            } catch (EOFException ignored) {
                // 旧格式，appliedIndex 默认为 0
            }
            // appliedIndex 作为 commitIndex 的下界保证：
            // 已 apply 的日志一定已 commit，所以 commitIndex >= appliedIndex
            return new long[]{currentTerm, votedFor, Math.max(commitIndex, appliedIndex), appliedIndex};
        }
    }

    /**
     * Truncate the log file up to the given index (after snapshotting).
     */
    public void truncateLogUpTo(long index) {
        // In a full implementation, this would compact the log file
        // For now, we keep all entries in memory
    }
}
