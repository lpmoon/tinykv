package io.tinykv.replication.async;

/**
 * Transport interface for async replication messages.
 */
public interface ReplicationTransport {
    void sendAsync(ReplicationMessage message, ReplicationCallback callback);
    void register(int nodeId, AsyncReplicator replicator);
    void start();
    void stop();
}
