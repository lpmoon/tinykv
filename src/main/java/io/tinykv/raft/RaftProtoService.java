package io.tinykv.raft;

import io.grpc.stub.StreamObserver;
import io.tinykv.proto.raft.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for Raft RPC.
 */
public class RaftProtoService extends RaftRpcServiceGrpc.RaftRpcServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(RaftProtoService.class);

    private final RaftNode raftNode;

    public RaftProtoService(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void handleRequestVote(RequestVoteRequest req, StreamObserver<RequestVoteResponse> observer) {
        LOG.debug("Received RequestVote from {}: term={}", req.getFromId(), req.getTerm());

        RaftMessage.RequestVote requestVote = new RaftMessage.RequestVote(
                req.getFromId(),
                req.getToId(),
                req.getTerm(),
                req.getLastLogIndex(),
                req.getLastLogTerm()
        );

        RaftMessage.RequestVoteResponse response = raftNode.handleRequestVote(requestVote);

        RequestVoteResponse grpcResponse = RequestVoteResponse.newBuilder()
                .setFromId(response.from())
                .setToId(response.to())
                .setTerm(response.term())
                .setVoteGranted(response.voteGranted())
                .build();

        observer.onNext(grpcResponse);
        observer.onCompleted();
    }

    @Override
    public void handleAppendEntries(AppendEntriesRequest req, StreamObserver<AppendEntriesResponse> observer) {
        LOG.debug("Received AppendEntries from {}: term={}, entries={}",
                req.getFromId(), req.getTerm(), req.getEntriesCount());

        // Convert LogEntry
        LogEntry[] entries = new LogEntry[req.getEntriesCount()];
        for (int i = 0; i < req.getEntriesCount(); i++) {
            io.tinykv.proto.raft.LogEntry grpcEntry = req.getEntries(i);
            entries[i] = new LogEntry(
                    grpcEntry.getTerm(),
                    grpcEntry.getIndex(),
                    grpcEntry.getData().toByteArray()
            );
        }

        RaftMessage.AppendEntries requestEntries = new RaftMessage.AppendEntries(
                req.getFromId(),
                req.getToId(),
                req.getTerm(),
                req.getPrevLogIndex(),
                req.getPrevLogTerm(),
                entries,
                req.getLeaderCommit()
        );

        RaftMessage.AppendEntriesResponse response = raftNode.handleAppendEntries(requestEntries);

        AppendEntriesResponse grpcResponse = AppendEntriesResponse.newBuilder()
                .setFromId(response.from())
                .setToId(response.to())
                .setTerm(response.term())
                .setSuccess(response.success())
                .setMatchIndex(response.matchIndex())
                .build();

        observer.onNext(grpcResponse);
        observer.onCompleted();
    }
}
