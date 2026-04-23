package io.tinykv.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Global configuration for TinyKV.
 */
public class Config {

    // Storage
    private String dataDir = "/tmp/tinykv/data";
    private long memTableSize = 64 * 1024 * 1024; // 64MB
    private int sstableBlockSize = 4 * 1024;       // 4KB
    private int blockSize = 4096;
    private int bloomFilterBitsPerKey = 10;

    // WAL
    private int walSyncBatchSize = 1024;

    // Compaction
    private int l0CompactionTrigger = 4;
    private int maxLevels = 7;
    private long levelSizeMultiplier = 10;

    // Cache
    private long blockCacheSize = 256 * 1024 * 1024; // 256MB

    // Raft
    private int raftElectionTimeoutMs = 1000;
    private int raftHeartbeatIntervalMs = 200;
    private int raftMaxEntriesPerAppend = 64;

    // Server
    private int port = 7000;
    private String peers = "";
    private String address = "";
    private String peerAddresses = "";
    private Map<Integer, String> peerAddressMap = new HashMap<>();

    // Cluster
    private String clusterName = "default";
    private String coordinatorAddress = "";

    // Replication
    private String replicationMode = "sync"; // "sync" or "async"
    private int waitReplicas = 0; // 0 = no wait (fire-and-forget)

    // Getters
    public String getDataDir() { return dataDir; }
    public long getMemTableSize() { return memTableSize; }
    public int getSstableBlockSize() { return sstableBlockSize; }
    public int getBlockSize() { return blockSize; }
    public int getBloomFilterBitsPerKey() { return bloomFilterBitsPerKey; }
    public int getWalSyncBatchSize() { return walSyncBatchSize; }
    public int getL0CompactionTrigger() { return l0CompactionTrigger; }
    public int getMaxLevels() { return maxLevels; }
    public long getLevelSizeMultiplier() { return levelSizeMultiplier; }
    public long getBlockCacheSize() { return blockCacheSize; }
    public int getRaftElectionTimeoutMs() { return raftElectionTimeoutMs; }
    public int getRaftHeartbeatIntervalMs() { return raftHeartbeatIntervalMs; }
    public int getRaftMaxEntriesPerAppend() { return raftMaxEntriesPerAppend; }
    public int getPort() { return port; }
    public String getPeers() { return peers; }

    // Getters
    public String getAddress() { return address; }
    public String getPeerAddresses() { return peerAddresses; }
    public String getClusterName() { return clusterName; }
    public String getCoordinatorAddress() { return coordinatorAddress; }
    public String getReplicationMode() { return replicationMode; }
    public int getWaitReplicas() { return waitReplicas; }

    public String getPeerAddress(int nodeId) {
        return peerAddressMap.get(nodeId);
    }

    public Map<Integer, String> getAllPeerAddresses() {
        return peerAddressMap;
    }

    // Setters
    public Config setDataDir(String dataDir) { this.dataDir = dataDir; return this; }
    public Config setMemTableSize(long memTableSize) { this.memTableSize = memTableSize; return this; }
    public Config setPort(int port) { this.port = port; return this; }
    public Config setPeers(String peers) { this.peers = peers; return this; }
    public Config setAddress(String address) { this.address = address; return this; }
    public Config setPeerAddresses(String peerAddresses) {
        this.peerAddresses = peerAddresses;
        parsePeerAddresses();
        return this;
    }
    public Config setClusterName(String clusterName) { this.clusterName = clusterName; return this; }
    public Config setCoordinatorAddress(String coordinatorAddress) { this.coordinatorAddress = coordinatorAddress; return this; }
    public Config setReplicationMode(String mode) { this.replicationMode = mode; return this; }
    public Config setWaitReplicas(int n) { this.waitReplicas = n; return this; }
    public Config setRaftElectionTimeoutMs(int ms) { this.raftElectionTimeoutMs = ms; return this; }
    public Config setRaftHeartbeatIntervalMs(int ms) { this.raftHeartbeatIntervalMs = ms; return this; }
    public Config setRaftMaxEntriesPerAppend(int max) { this.raftMaxEntriesPerAppend = max; return this; }

    private void parsePeerAddresses() {
        peerAddressMap.clear();
        if (peerAddresses == null || peerAddresses.isEmpty()) {
            return;
        }
        String[] parts = peerAddresses.split(",");
        for (String part : parts) {
            String[] addrParts = part.trim().split(":");
            if (addrParts.length >= 3) {
                int nodeId = Integer.parseInt(addrParts[0]);
                String addr = addrParts[1] + ":" + addrParts[2];
                peerAddressMap.put(nodeId, addr);
            }
        }
    }
}
