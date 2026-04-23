package io.tinykv.storage.memtable;

/**
 * Factory for creating MemTable instances.
 */
public class MemTableFactory {

    private MemTableFactory() {}

    /**
     * Create a new MemTable of the specified type.
     */
    public static MemTable create(MemTableType type) {
        switch (type) {
            case SKIP_LIST:
                return new SkipListMemTable();
            case VECTOR:
                return new VectorMemTable();
            case HASH_SKIP_LIST:
                return new HashSkipListMemTable();
            default:
                throw new IllegalArgumentException("Unknown memtable type: " + type);
        }
    }

    /**
     * Create a new MemTable of the specified type with custom parameters.
     */
    public static MemTable create(MemTableType type, int bucketCount) {
        if (type == MemTableType.HASH_SKIP_LIST) {
            return new HashSkipListMemTable(bucketCount);
        }
        return create(type);
    }
}
