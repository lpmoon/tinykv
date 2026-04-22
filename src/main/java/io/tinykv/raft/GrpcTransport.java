package io.tinykv.raft;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tinykv.common.Config;
import io.tinykv.proto.raft.*;
import io.tinykv.server.TinyKVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * gRPC-based transport for Raft message passing between nodes.
 * Supports both server (listening for requests) and client (sending requests).
 */
public class GrpcTransport implements RaftTransport {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcTransport.class);

    private final Config config;
    private final Map<Integer, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Map<Integer, RaftRpcServiceGrpc.RaftRpcServiceBlockingStub> stubs = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "raft-grpc-transport");
        t.setDaemon(true);
        return t;
    });

    private Server server;
    private ManagedChannel channel;
    private RaftRpcServiceGrpc.RaftRpcServiceBlockingStub syncStub;
    private TinyKVService kvService;

    public GrpcTransport(Config config) {
        this.config = config;
    }

    @Override
    public void sendAsync(RaftMessage message, RaftMessageCallback callback) {
        executor.submit(() -> {
            RaftMessage response = sendSync(message, 5000);
            if (callback != null && response != null) {
                callback.onResponse(response);
            }
        });
    }

    @Override
    public RaftMessage sendSync(RaftMessage message, long timeoutMs) {
        try {
            if (message instanceof RaftMessage.RequestVote req) {
                return sendRequestVote(req, timeoutMs);
            } else if (message instanceof RaftMessage.AppendEntries req) {
                return sendAppendEntries(req, timeoutMs);
            } else {
                LOG.warn("Unknown message type: {}", message.getClass());
                return null;
            }
        } catch (Exception e) {
            LOG.error("Failed to send message to {}: {}", message.to(), e.getMessage());
            return null;
        }
    }

    private RaftMessage sendRequestVote(RaftMessage.RequestVote req, long timeoutMs) {
        RequestVoteRequest grpcReq = RequestVoteRequest.newBuilder()
                .setFromId(req.from())
                .setToId(req.to())
                .setTerm(req.term())
                .setLastLogIndex(req.lastLogIndex())
                .setLastLogTerm(req.lastLogTerm())
                .build();

        RequestVoteResponse response = getStub(req.to()).handleRequestVote(grpcReq);

        return new RaftMessage.RequestVoteResponse(
                response.getFromId(),
                response.getToId(),
                response.getTerm(),
                response.getVoteGranted()
        );
    }

    private RaftMessage sendAppendEntries(RaftMessage.AppendEntries req, long timeoutMs) {
        io.tinykv.proto.raft.LogEntry[] grpcEntries = new io.tinykv.proto.raft.LogEntry[req.entries().length];
        for (int i = 0; i < req.entries().length; i++) {
            LogEntry entry = req.entries()[i];
            grpcEntries[i] = io.tinykv.proto.raft.LogEntry.newBuilder()
                    .setTerm(entry.term())
                    .setIndex(entry.index())
                    .setData(com.google.protobuf.ByteString.copyFrom(entry.data()))
                    .build();
        }

        AppendEntriesRequest grpcReq = AppendEntriesRequest.newBuilder()
                .setFromId(req.from())
                .setToId(req.to())
                .setTerm(req.term())
                .setPrevLogIndex(req.prevLogIndex())
                .setPrevLogTerm(req.prevLogTerm())
                .addAllEntries(java.util.Arrays.asList(grpcEntries))
                .setLeaderCommit(req.leaderCommit())
                .build();

        AppendEntriesResponse response = getStub(req.to()).handleAppendEntries(grpcReq);

        return new RaftMessage.AppendEntriesResponse(
                response.getFromId(),
                response.getToId(),
                response.getTerm(),
                response.getSuccess(),
                response.getMatchIndex()
        );
    }

    private RaftRpcServiceGrpc.RaftRpcServiceBlockingStub getStub(int nodeId) {
        return stubs.computeIfAbsent(nodeId, id -> {
            String address = config.getPeerAddress(id);
            if (address == null) {
                throw new IllegalArgumentException("Unknown node ID: " + nodeId);
            }
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address)
                    .usePlaintext()
                    .build();
            return RaftRpcServiceGrpc.newBlockingStub(channel);
        });
    }

    @Override
    public void register(int nodeId, RaftNode node) {
        nodes.put(nodeId, node);
        LOG.info("Registered RaftNode {} for RPC", nodeId);
    }

    public void setKVService(TinyKVService kvService) {
        this.kvService = kvService;
    }

    @Override
    public void start() {
        try {
            String address = config.getAddress();
            int port = address.contains(":") ?
                    Integer.parseInt(address.split(":")[1]) : config.getPort();

            ServerBuilder<?> builder = ServerBuilder.forPort(port);

            // Register Raft service
            if (!nodes.isEmpty()) {
                builder.addService(new RaftProtoService(nodes.values().iterator().next()));
            }

            // Register KV service
            if (kvService != null) {
                int nodeId = parseNodeId(config);
                RaftNode raftNode = nodes.get(nodeId);
                if (raftNode != null) {
                    builder.addService(new KVProtoService(raftNode, kvService, config));
                }
            }

            server = builder.build().start();

            LOG.info("gRPC server started on port {}", port);

            // Pre-build channels to peers
            for (Map.Entry<Integer, String> entry : config.getAllPeerAddresses().entrySet()) {
                int peerId = entry.getKey();
                String peerAddr = entry.getValue();
                ManagedChannel channel = ManagedChannelBuilder.forTarget(peerAddr)
                        .usePlaintext()
                        .build();
                stubs.put(peerId, RaftRpcServiceGrpc.newBlockingStub(channel));
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to start gRPC server", e);
        }
    }

    private int parseNodeId(Config config) {
        Map<Integer, String> peerAddresses = config.getAllPeerAddresses();
        String ourAddress = config.getAddress();

        for (Map.Entry<Integer, String> entry : peerAddresses.entrySet()) {
            if (entry.getValue().equals(ourAddress)) {
                return entry.getKey();
            }
        }

        String[] parts = ourAddress.split(":");
        if (parts.length >= 3) {
            try {
                return Integer.parseInt(parts[0]);
            } catch (NumberFormatException e) {
                // Ignore
            }
        }

        return Integer.parseInt(parts[parts.length - 1]) % 100;
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("gRPC transport stopped");
    }
}
