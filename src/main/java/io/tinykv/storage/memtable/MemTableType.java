package io.tinykv.storage.memtable;

/**
 * MemTable types supported by TinyKV.
 */
public enum MemTableType {
    SKIP_LIST,
    VECTOR,
    HASH_SKIP_LIST
}
