package io.tinykv.replication.async;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tinykv.common.Config;
import io.tinykv.proto.replication.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * gRPC transport for async replication messages.
 */
public class GrpcReplicationTransport implements ReplicationTransport {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcReplicationTransport.class);

    private final Config config;
    private final Map<Integer, AsyncReplicator> replicators = new ConcurrentHashMap<>();
    private final Map<Integer, ReplicationRpcServiceGrpc.ReplicationRpcServiceBlockingStub> stubs = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "repl-grpc-transport");
        t.setDaemon(true);
        return t;
    });

    private Server server;

    public GrpcReplicationTransport(Config config) {
        this.config = config;
    }

    @Override
    public void sendAsync(ReplicationMessage message, ReplicationCallback callback) {
        executor.submit(() -> {
            if (message instanceof ReplicationMessage.Heartbeat req) {
                sendHeartbeat(req, callback);
            } else if (message instanceof ReplicationMessage.ReplicateEntries req) {
                sendReplicateEntries(req, callback);
            } else {
                LOG.warn("Unknown replication message type: {}", message.getClass());
            }
        });
    }

    private void sendHeartbeat(ReplicationMessage.Heartbeat req, ReplicationCallback callback) {
        try {
            ReplHeartbeatRequest grpcReq = ReplHeartbeatRequest.newBuilder()
                    .setFromId(req.from())
                    .setToId(req.to())
                    .setEpoch(req.epoch())
                    .build();

            ReplHeartbeatResponse grpcResp = getStub(req.to()).handleHeartbeat(grpcReq);

            ReplicationMessage.HeartbeatResponse response = new ReplicationMessage.HeartbeatResponse(
                    grpcResp.getFromId(), grpcResp.getToId(), grpcResp.getEpoch(), grpcResp.getVoteGranted());

            if (callback != null) {
                callback.onResponse(response);
            }
        } catch (Exception e) {
            LOG.error("Failed to send Heartbeat to {}: {}", req.to(), e.getMessage());
        }
    }

    private void sendReplicateEntries(ReplicationMessage.ReplicateEntries req, ReplicationCallback callback) {
        try {
            ReplicationLogEntry[] grpcEntries = new ReplicationLogEntry[req.entries().length];
            for (int i = 0; i < req.entries().length; i++) {
                ReplicationEntry entry = req.entries()[i];
                grpcEntries[i] = ReplicationLogEntry.newBuilder()
                        .setIndex(entry.index())
                        .setData(com.google.protobuf.ByteString.copyFrom(entry.data()))
                        .build();
            }

            ReplicateEntriesRequest grpcReq = ReplicateEntriesRequest.newBuilder()
                    .setFromId(req.from())
                    .setToId(req.to())
                    .setPrevIndex(req.prevIndex())
                    .addAllEntries(java.util.Arrays.asList(grpcEntries))
                    .build();

            ReplicateEntriesResponse grpcResp = getStub(req.to()).handleReplicateEntries(grpcReq);

            ReplicationMessage.ReplicateEntriesResponse response =
                    new ReplicationMessage.ReplicateEntriesResponse(
                            grpcResp.getFromId(), grpcResp.getToId(),
                            grpcResp.getSuccess(), grpcResp.getMatchIndex());

            if (callback != null) {
                callback.onResponse(response);
            }
        } catch (Exception e) {
            LOG.error("Failed to send ReplicateEntries to {}: {}", req.to(), e.getMessage());
        }
    }

    private ReplicationRpcServiceGrpc.ReplicationRpcServiceBlockingStub getStub(int nodeId) {
        return stubs.computeIfAbsent(nodeId, id -> {
            String address = config.getPeerAddress(id);
            if (address == null) {
                throw new IllegalArgumentException("Unknown node ID: " + id);
            }
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address)
                    .usePlaintext()
                    .build();
            return ReplicationRpcServiceGrpc.newBlockingStub(channel);
        });
    }

    @Override
    public void register(int nodeId, AsyncReplicator replicator) {
        replicators.put(nodeId, replicator);
        LOG.info("Registered AsyncReplicator {} for RPC", nodeId);
    }

    @Override
    public void start() {
        try {
            String address = config.getAddress();
            int port = address.contains(":") ?
                    Integer.parseInt(address.split(":")[1]) : config.getPort();

            int replPort = port + 1000;

            ServerBuilder<?> builder = ServerBuilder.forPort(replPort);

            if (!replicators.isEmpty()) {
                builder.addService(new ReplicationProtoService(replicators.values().iterator().next()));
            }

            server = builder.build().start();
            LOG.info("Replication gRPC server started on port {}", replPort);

            for (Map.Entry<Integer, String> entry : config.getAllPeerAddresses().entrySet()) {
                int peerId = entry.getKey();
                String peerAddr = entry.getValue();
                String[] parts = peerAddr.split(":");
                int peerReplPort = Integer.parseInt(parts[1]) + 1000;
                String replAddr = parts[0] + ":" + peerReplPort;

                ManagedChannel channel = ManagedChannelBuilder.forTarget(replAddr)
                        .usePlaintext()
                        .build();
                stubs.put(peerId, ReplicationRpcServiceGrpc.newBlockingStub(channel));
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to start replication gRPC server", e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("Replication gRPC transport stopped");
    }
}
