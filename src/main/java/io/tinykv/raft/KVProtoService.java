package io.tinykv.raft;

import io.grpc.stub.StreamObserver;
import io.tinykv.common.Config;
import io.tinykv.proto.kv.*;
import io.tinykv.server.TinyKVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * gRPC service implementation for KV operations.
 * Every node serves this RPC. Non-leader nodes return leader hints
 * so clients can redirect.
 */
public class KVProtoService extends KVServiceGrpc.KVServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(KVProtoService.class);

    private final RaftNode raftNode;
    private final TinyKVService kvService;
    private final Config config;

    public KVProtoService(RaftNode raftNode, TinyKVService kvService, Config config) {
        this.raftNode = raftNode;
        this.kvService = kvService;
        this.config = config;
    }

    private boolean isLeader() {
        return raftNode.isLeader();
    }

    private String getLeaderAddress() {
        int leaderId = raftNode.getLeaderId();
        if (leaderId < 0) {
            return "";
        }
        return config.getPeerAddress(leaderId);
    }

    private int getLeaderId() {
        return raftNode.getLeaderId();
    }

    private <T> void redirectToLeader(StreamObserver<T> observer) {
        GetResponse.Builder resp = GetResponse.newBuilder();
        resp.setLeaderAddress(getLeaderAddress());
        resp.setLeaderId(getLeaderId());
        observer.onError(new NotLeaderException(getLeaderAddress(), getLeaderId()));
    }

    @Override
    public void get(GetRequest req, StreamObserver<GetResponse> observer) {
        LOG.debug("KV Get: key={}", java.util.Arrays.toString(req.getKey().toByteArray()));

        if (!isLeader()) {
            LOG.debug("Not leader, redirecting to {}", getLeaderAddress());
            GetResponse resp = GetResponse.newBuilder()
                    .setLeaderAddress(getLeaderAddress())
                    .setLeaderId(getLeaderId())
                    .build();
            observer.onNext(resp);
            observer.onCompleted();
            return;
        }

        try {
            java.util.Optional<byte[]> value = kvService.get(req.getKey().toByteArray());
            GetResponse.Builder resp = GetResponse.newBuilder();
            if (value.isPresent()) {
                resp.setFound(true);
                resp.setValue(com.google.protobuf.ByteString.copyFrom(value.get()));
            } else {
                resp.setFound(false);
            }
            observer.onNext(resp.build());
            observer.onCompleted();
        } catch (NotLeaderException e) {
            GetResponse resp = GetResponse.newBuilder()
                    .setLeaderAddress(e.leaderAddress)
                    .setLeaderId(e.leaderId)
                    .build();
            observer.onNext(resp);
            observer.onCompleted();
        }
    }

    @Override
    public void put(PutRequest req, StreamObserver<PutResponse> observer) {
        LOG.debug("KV Put: key={}, value={}", java.util.Arrays.toString(req.getKey().toByteArray()), req.getValue().size());

        if (!isLeader()) {
            LOG.debug("Not leader, redirecting to {}", getLeaderAddress());
            PutResponse resp = PutResponse.newBuilder()
                    .setLeaderAddress(getLeaderAddress())
                    .setLeaderId(getLeaderId())
                    .build();
            observer.onNext(resp);
            observer.onCompleted();
            return;
        }

        try {
            kvService.put(req.getKey().toByteArray(), req.getValue().toByteArray());
            PutResponse resp = PutResponse.newBuilder()
                    .setOk(true)
                    .build();
            observer.onNext(resp);
            observer.onCompleted();
        } catch (NotLeaderException e) {
            PutResponse resp = PutResponse.newBuilder()
                    .setLeaderAddress(e.leaderAddress)
                    .setLeaderId(e.leaderId)
                    .build();
            observer.onNext(resp);
            observer.onCompleted();
        } catch (Exception e) {
            LOG.error("Put failed", e);
            observer.onError(e);
        }
    }

    @Override
    public void delete(DeleteRequest req, StreamObserver<DeleteResponse> observer) {
        LOG.debug("KV Delete: key={}", java.util.Arrays.toString(req.getKey().toByteArray()));

        if (!isLeader()) {
            LOG.debug("Not leader, redirecting to {}", getLeaderAddress());
            DeleteResponse resp = DeleteResponse.newBuilder()
                    .setLeaderAddress(getLeaderAddress())
                    .setLeaderId(getLeaderId())
                    .build();
            observer.onNext(resp);
            observer.onCompleted();
            return;
        }

        try {
            kvService.delete(req.getKey().toByteArray());
            DeleteResponse resp = DeleteResponse.newBuilder()
                    .setOk(true)
                    .build();
            observer.onNext(resp);
            observer.onCompleted();
        } catch (NotLeaderException e) {
            DeleteResponse resp = DeleteResponse.newBuilder()
                    .setLeaderAddress(e.leaderAddress)
                    .setLeaderId(e.leaderId)
                    .build();
            observer.onNext(resp);
            observer.onCompleted();
        } catch (Exception e) {
            LOG.error("Delete failed", e);
            observer.onError(e);
        }
    }

    @Override
    public void scan(ScanRequest req, StreamObserver<ScanEntry> observer) {
        LOG.debug("KV Scan: start={}, end={}, limit={}",
                java.util.Arrays.toString(req.getStartKey().toByteArray()),
                java.util.Arrays.toString(req.getEndKey().toByteArray()),
                req.getLimit());

        if (!isLeader()) {
            observer.onError(new NotLeaderException(getLeaderAddress(), getLeaderId()));
            return;
        }

        try {
            java.util.Iterator<TinyKVService.KVEntry> iter = kvService.scan(
                    req.getStartKey().toByteArray(),
                    req.getEndKey().toByteArray()
            );

            int count = 0;
            int limit = (req.getLimit() > 0) ? req.getLimit() : Integer.MAX_VALUE;

            while (iter.hasNext() && count < limit) {
                TinyKVService.KVEntry entry = iter.next();
                ScanEntry resp = ScanEntry.newBuilder()
                        .setKey(com.google.protobuf.ByteString.copyFrom(entry.key()))
                        .setValue(com.google.protobuf.ByteString.copyFrom(entry.value()))
                        .build();
                observer.onNext(resp);
                count++;
            }
            observer.onCompleted();
        } catch (Exception e) {
            LOG.error("Scan failed", e);
            observer.onError(e);
        }
    }

    @Override
    public void getClusterInfo(ClusterInfoRequest req, StreamObserver<ClusterInfoResponse> observer) {
        LOG.debug("KV GetClusterInfo");

        ClusterInfoResponse.Builder resp = ClusterInfoResponse.newBuilder();
        resp.setLeaderId(getLeaderId());
        resp.setLeaderAddress(getLeaderAddress());

        Map<Integer, String> peerAddresses = config.getAllPeerAddresses();
        for (Map.Entry<Integer, String> entry : peerAddresses.entrySet()) {
            resp.putPeerAddresses(entry.getKey(), entry.getValue());
        }

        observer.onNext(resp.build());
        observer.onCompleted();
    }

    public static class NotLeaderException extends RuntimeException {
        public final String leaderAddress;
        public final int leaderId;

        public NotLeaderException(String leaderAddress, int leaderId) {
            super("Not leader. Leader: " + leaderAddress + " (id=" + leaderId + ")");
            this.leaderAddress = leaderAddress;
            this.leaderId = leaderId;
        }
    }
}
