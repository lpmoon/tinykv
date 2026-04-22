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
     * Take a snapshot of the current state machine.
     */
    byte[] snapshot();

    /**
     * Restore state machine from a snapshot.
     */
    void restore(byte[] snapshot);
}
