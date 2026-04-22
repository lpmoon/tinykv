package io.tinykv.coordinator;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.tinykv.proto.coordinator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC server implementation for the Coordinator service.
 */
public class CoordinatorGrpcServer {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcServer.class);

    private final CoordinatorService coordinatorService;
    private final String bindAddress;
    private final int port;
    private Server server;

    public CoordinatorGrpcServer(CoordinatorService coordinatorService, String bindAddress, int port) {
        this.coordinatorService = coordinatorService;
        this.bindAddress = bindAddress;
        this.port = port;
    }

    public void start() {
        server = ServerBuilder.forPort(port)
                .addService(new CoordinatorRpcService())
                .build();

        try {
            server.start();
            coordinatorService.start();
            LOG.info("Coordinator gRPC server started on port {}", port);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Coordinator gRPC server", e);
        }
    }

    public void stop() {
        coordinatorService.stop();
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private class CoordinatorRpcService extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {

        @Override
        public void registerNode(RegisterNodeRequest req, StreamObserver<RegisterNodeResponse> observer) {
            try {
                RegisterNodeResponse resp = coordinatorService.registerNode(req);
                observer.onNext(resp);
                observer.onCompleted();
            } catch (Exception e) {
                LOG.error("registerNode failed", e);
                observer.onError(e);
            }
        }

        @Override
        public void heartbeat(HeartbeatRequest req, StreamObserver<HeartbeatResponse> observer) {
            try {
                HeartbeatResponse resp = coordinatorService.heartbeat(req);
                observer.onNext(resp);
                observer.onCompleted();
            } catch (Exception e) {
                LOG.error("heartbeat failed", e);
                observer.onError(e);
            }
        }

        @Override
        public void getLeader(GetLeaderRequest req, StreamObserver<GetLeaderResponse> observer) {
            try {
                GetLeaderResponse resp = coordinatorService.getLeader(req);
                observer.onNext(resp);
                observer.onCompleted();
            } catch (Exception e) {
                LOG.error("getLeader failed", e);
                observer.onError(e);
            }
        }

        @Override
        public void getClusterInfo(GetClusterInfoRequest req, StreamObserver<GetClusterInfoResponse> observer) {
            try {
                GetClusterInfoResponse resp = coordinatorService.getClusterInfo(req);
                observer.onNext(resp);
                observer.onCompleted();
            } catch (Exception e) {
                LOG.error("getClusterInfo failed", e);
                observer.onError(e);
            }
        }
    }
}
