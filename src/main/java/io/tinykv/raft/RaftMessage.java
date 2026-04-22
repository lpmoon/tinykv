package io.tinykv.raft;

/**
 * Messages exchanged between Raft nodes.
 */
public sealed interface RaftMessage {

    int from();
    int to();

    /**
     * RequestVote RPC — sent by candidates to request votes.
     */
    record RequestVote(
            int from, int to,
            long term,
            long lastLogIndex, long lastLogTerm
    ) implements RaftMessage {}

    /**
     * RequestVote response.
     */
    record RequestVoteResponse(
            int from, int to,
            long term,
            boolean voteGranted
    ) implements RaftMessage {}

    /**
     * AppendEntries RPC — sent by leader to replicate log entries and serve as heartbeat.
     */
    record AppendEntries(
            int from, int to,
            long term,
            long prevLogIndex, long prevLogTerm,
            LogEntry[] entries,
            long leaderCommit
    ) implements RaftMessage {}

    /**
     * AppendEntries response.
     */
    record AppendEntriesResponse(
            int from, int to,
            long term,
            boolean success,
            long matchIndex
    ) implements RaftMessage {}
}
