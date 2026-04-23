package io.tinykv.replication.async;

/**
 * Messages exchanged between AsyncReplicator nodes.
 */
public sealed interface ReplicationMessage {

    int from();
    int to();

    record Heartbeat(int from, int to, long epoch) implements ReplicationMessage {}

    record HeartbeatResponse(int from, int to, long epoch, boolean voteGranted) implements ReplicationMessage {}

    record ReplicateEntries(int from, int to, long prevIndex, ReplicationEntry[] entries) implements ReplicationMessage {}

    record ReplicateEntriesResponse(int from, int to, boolean success, long matchIndex) implements ReplicationMessage {}
}
