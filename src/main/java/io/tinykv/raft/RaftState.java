package io.tinykv.raft;

/**
 * Raft node role.
 */
public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
