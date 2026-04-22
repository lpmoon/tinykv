package io.tinykv.transaction;

import io.tinykv.common.Config;
import io.tinykv.raft.*;
import io.tinykv.replication.*;
import io.tinykv.storage.*;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TxnTest {

    private static final String TEST_DIR = "/tmp/tinykv-test-txn";

    @Test
    void testMVCCKeyEncoding() {
        byte[] userKey = "mykey".getBytes();
        long commitTs = 100L;

        MVCCKey original = new MVCCKey(userKey, commitTs);
        byte[] encoded = original.encode();
        MVCCKey decoded = MVCCKey.decode(encoded);

        assertArrayEquals(userKey, decoded.getUserKey());
        assertEquals(commitTs, decoded.getCommitTs());
    }

    @Test
    void testMVCCKeySortOrder() {
        byte[] userKey = "mykey".getBytes();

        MVCCKey v1 = new MVCCKey(userKey, 100L);
        MVCCKey v2 = new MVCCKey(userKey, 200L);
        MVCCKey v3 = new MVCCKey(userKey, 300L);

        // Newer timestamps should sort first (descending)
        assertTrue(v3.compareTo(v2) < 0, "v3 (ts=300) should sort before v2 (ts=200)");
        assertTrue(v2.compareTo(v1) < 0, "v2 (ts=200) should sort before v1 (ts=100)");
    }

    @Test
    void testTimestampOracle() {
        TimestampOracle oracle = new TimestampOracle();

        long ts1 = oracle.next();
        long ts2 = oracle.next();
        long ts3 = oracle.next();

        assertTrue(ts1 < ts2);
        assertTrue(ts2 < ts3);
        assertEquals(ts3, oracle.current());
    }
}
