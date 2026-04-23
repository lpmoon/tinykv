package io.tinykv.replication.async;

import io.grpc.stub.StreamObserver;
import io.tinykv.proto.replication.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for async replication RPC.
 */
public class ReplicationProtoService extends ReplicationRpcServiceGrpc.ReplicationRpcServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationProtoService.class);

    private final AsyncReplicator replicator;

    public ReplicationProtoService(AsyncReplicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public void handleHeartbeat(ReplHeartbeatRequest req, StreamObserver<ReplHeartbeatResponse> observer) {
        ReplicationMessage.Heartbeat hb = new ReplicationMessage.Heartbeat(
                req.getFromId(), req.getToId(), req.getEpoch());

        ReplicationMessage.HeartbeatResponse resp = replicator.handleHeartbeat(hb);

        ReplHeartbeatResponse grpcResp = ReplHeartbeatResponse.newBuilder()
                .setFromId(resp.from())
                .setToId(resp.to())
                .setEpoch(resp.epoch())
                .setVoteGranted(resp.voteGranted())
                .build();

        observer.onNext(grpcResp);
        observer.onCompleted();
    }

    @Override
    public void handleReplicateEntries(ReplicateEntriesRequest req,
                                       StreamObserver<ReplicateEntriesResponse> observer) {
        ReplicationEntry[] entries = new ReplicationEntry[req.getEntriesCount()];
        for (int i = 0; i < req.getEntriesCount(); i++) {
            ReplicationLogEntry grpcEntry = req.getEntries(i);
            entries[i] = new ReplicationEntry(
                    grpcEntry.getIndex(),
                    grpcEntry.getData().toByteArray()
            );
        }

        ReplicationMessage.ReplicateEntries msg = new ReplicationMessage.ReplicateEntries(
                req.getFromId(), req.getToId(), req.getPrevIndex(), entries);

        ReplicationMessage.ReplicateEntriesResponse response = replicator.handleReplicateEntries(msg);

        ReplicateEntriesResponse grpcResponse = ReplicateEntriesResponse.newBuilder()
                .setFromId(response.from())
                .setToId(response.to())
                .setSuccess(response.success())
                .setMatchIndex(response.matchIndex())
                .build();

        observer.onNext(grpcResponse);
        observer.onCompleted();
    }
}
