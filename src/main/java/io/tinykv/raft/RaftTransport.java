package io.tinykv.raft;

/**
 * Transport interface for sending Raft messages between nodes.
 */
public interface RaftTransport {

    /**
     * Send a message asynchronously. The callback will be invoked with the response.
     */
    void sendAsync(RaftMessage message, RaftMessageCallback callback);

    /**
     * Send a message and wait for the response synchronously.
     */
    RaftMessage sendSync(RaftMessage message, long timeoutMs);

    /**
     * Register a RaftNode to receive messages for a given node ID.
     */
    void register(int nodeId, RaftNode node);

    /**
     * Start the transport (e.g., start gRPC server).
     */
    void start();

    /**
     * Stop the transport.
     */
    void stop();
}

/**
 * Callback for async Raft message responses.
 */
@FunctionalInterface
interface RaftMessageCallback {
    void onResponse(RaftMessage response);
}
