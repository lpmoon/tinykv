package io.tinykv.transaction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides monotonically increasing timestamps for MVCC.
 * In a single-node setup, a simple atomic counter suffices.
 * In a distributed setup, this would be provided by a placement driver (PD)
 * or use a hybrid logical clock (HLC).
 */
public class TimestampOracle {

    private final AtomicLong currentTs = new AtomicLong(0);

    /**
     * Get the next timestamp.
     */
    public long next() {
        return currentTs.incrementAndGet();
    }

    /**
     * Get the current timestamp without advancing.
     */
    public long current() {
        return currentTs.get();
    }

    /**
     * Initialize from a saved value (for recovery).
     */
    public void init(long startTs) {
        currentTs.set(startTs);
    }
}
