package io.tinykv.raft;

/**
 * State machine interface. Applied commands update the state machine.
 */
public interface StateMachine {

    /**
     * Apply a committed log entry to the state machine.
     * Returns the result of the application (may be null for writes).
     */
    byte[] apply(byte[] command);

    /**
     * Read a key from the state machine.
     * Used by AsyncReplicator for eventually-consistent reads.
     * Returns null if key not found.
     */
    default byte[] get(byte[] key) { return null; }

    /**
     * Take a snapshot of the current state machine.
     */
    byte[] snapshot();

    /**
     * Restore state machine from a snapshot.
     */
    void restore(byte[] snapshot);
}
