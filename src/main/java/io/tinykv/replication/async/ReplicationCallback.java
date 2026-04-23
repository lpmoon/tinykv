package io.tinykv.replication.async;

@FunctionalInterface
interface ReplicationCallback {
    void onResponse(ReplicationMessage response);
}
