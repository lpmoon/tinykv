package io.tinykv.proto.raft;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * gRPC service definition
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.62.2)",
    comments = "Source: Raft.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class RaftRpcServiceGrpc {

  private RaftRpcServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "tinykv.RaftRpcService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.tinykv.proto.raft.RequestVoteRequest,
      io.tinykv.proto.raft.RequestVoteResponse> getHandleRequestVoteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HandleRequestVote",
      requestType = io.tinykv.proto.raft.RequestVoteRequest.class,
      responseType = io.tinykv.proto.raft.RequestVoteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.tinykv.proto.raft.RequestVoteRequest,
      io.tinykv.proto.raft.RequestVoteResponse> getHandleRequestVoteMethod() {
    io.grpc.MethodDescriptor<io.tinykv.proto.raft.RequestVoteRequest, io.tinykv.proto.raft.RequestVoteResponse> getHandleRequestVoteMethod;
    if ((getHandleRequestVoteMethod = RaftRpcServiceGrpc.getHandleRequestVoteMethod) == null) {
      synchronized (RaftRpcServiceGrpc.class) {
        if ((getHandleRequestVoteMethod = RaftRpcServiceGrpc.getHandleRequestVoteMethod) == null) {
          RaftRpcServiceGrpc.getHandleRequestVoteMethod = getHandleRequestVoteMethod =
              io.grpc.MethodDescriptor.<io.tinykv.proto.raft.RequestVoteRequest, io.tinykv.proto.raft.RequestVoteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HandleRequestVote"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.tinykv.proto.raft.RequestVoteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.tinykv.proto.raft.RequestVoteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RaftRpcServiceMethodDescriptorSupplier("HandleRequestVote"))
              .build();
        }
      }
    }
    return getHandleRequestVoteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.tinykv.proto.raft.AppendEntriesRequest,
      io.tinykv.proto.raft.AppendEntriesResponse> getHandleAppendEntriesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HandleAppendEntries",
      requestType = io.tinykv.proto.raft.AppendEntriesRequest.class,
      responseType = io.tinykv.proto.raft.AppendEntriesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.tinykv.proto.raft.AppendEntriesRequest,
      io.tinykv.proto.raft.AppendEntriesResponse> getHandleAppendEntriesMethod() {
    io.grpc.MethodDescriptor<io.tinykv.proto.raft.AppendEntriesRequest, io.tinykv.proto.raft.AppendEntriesResponse> getHandleAppendEntriesMethod;
    if ((getHandleAppendEntriesMethod = RaftRpcServiceGrpc.getHandleAppendEntriesMethod) == null) {
      synchronized (RaftRpcServiceGrpc.class) {
        if ((getHandleAppendEntriesMethod = RaftRpcServiceGrpc.getHandleAppendEntriesMethod) == null) {
          RaftRpcServiceGrpc.getHandleAppendEntriesMethod = getHandleAppendEntriesMethod =
              io.grpc.MethodDescriptor.<io.tinykv.proto.raft.AppendEntriesRequest, io.tinykv.proto.raft.AppendEntriesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HandleAppendEntries"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.tinykv.proto.raft.AppendEntriesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.tinykv.proto.raft.AppendEntriesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RaftRpcServiceMethodDescriptorSupplier("HandleAppendEntries"))
              .build();
        }
      }
    }
    return getHandleAppendEntriesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RaftRpcServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftRpcServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftRpcServiceStub>() {
        @java.lang.Override
        public RaftRpcServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftRpcServiceStub(channel, callOptions);
        }
      };
    return RaftRpcServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RaftRpcServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftRpcServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftRpcServiceBlockingStub>() {
        @java.lang.Override
        public RaftRpcServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftRpcServiceBlockingStub(channel, callOptions);
        }
      };
    return RaftRpcServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RaftRpcServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RaftRpcServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RaftRpcServiceFutureStub>() {
        @java.lang.Override
        public RaftRpcServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RaftRpcServiceFutureStub(channel, callOptions);
        }
      };
    return RaftRpcServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * gRPC service definition
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default void handleRequestVote(io.tinykv.proto.raft.RequestVoteRequest request,
        io.grpc.stub.StreamObserver<io.tinykv.proto.raft.RequestVoteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHandleRequestVoteMethod(), responseObserver);
    }

    /**
     */
    default void handleAppendEntries(io.tinykv.proto.raft.AppendEntriesRequest request,
        io.grpc.stub.StreamObserver<io.tinykv.proto.raft.AppendEntriesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHandleAppendEntriesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service RaftRpcService.
   * <pre>
   * gRPC service definition
   * </pre>
   */
  public static abstract class RaftRpcServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RaftRpcServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service RaftRpcService.
   * <pre>
   * gRPC service definition
   * </pre>
   */
  public static final class RaftRpcServiceStub
      extends io.grpc.stub.AbstractAsyncStub<RaftRpcServiceStub> {
    private RaftRpcServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftRpcServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftRpcServiceStub(channel, callOptions);
    }

    /**
     */
    public void handleRequestVote(io.tinykv.proto.raft.RequestVoteRequest request,
        io.grpc.stub.StreamObserver<io.tinykv.proto.raft.RequestVoteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHandleRequestVoteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void handleAppendEntries(io.tinykv.proto.raft.AppendEntriesRequest request,
        io.grpc.stub.StreamObserver<io.tinykv.proto.raft.AppendEntriesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHandleAppendEntriesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service RaftRpcService.
   * <pre>
   * gRPC service definition
   * </pre>
   */
  public static final class RaftRpcServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RaftRpcServiceBlockingStub> {
    private RaftRpcServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftRpcServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftRpcServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.tinykv.proto.raft.RequestVoteResponse handleRequestVote(io.tinykv.proto.raft.RequestVoteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHandleRequestVoteMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.tinykv.proto.raft.AppendEntriesResponse handleAppendEntries(io.tinykv.proto.raft.AppendEntriesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHandleAppendEntriesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service RaftRpcService.
   * <pre>
   * gRPC service definition
   * </pre>
   */
  public static final class RaftRpcServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<RaftRpcServiceFutureStub> {
    private RaftRpcServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftRpcServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RaftRpcServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.tinykv.proto.raft.RequestVoteResponse> handleRequestVote(
        io.tinykv.proto.raft.RequestVoteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHandleRequestVoteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.tinykv.proto.raft.AppendEntriesResponse> handleAppendEntries(
        io.tinykv.proto.raft.AppendEntriesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHandleAppendEntriesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_HANDLE_REQUEST_VOTE = 0;
  private static final int METHODID_HANDLE_APPEND_ENTRIES = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_HANDLE_REQUEST_VOTE:
          serviceImpl.handleRequestVote((io.tinykv.proto.raft.RequestVoteRequest) request,
              (io.grpc.stub.StreamObserver<io.tinykv.proto.raft.RequestVoteResponse>) responseObserver);
          break;
        case METHODID_HANDLE_APPEND_ENTRIES:
          serviceImpl.handleAppendEntries((io.tinykv.proto.raft.AppendEntriesRequest) request,
              (io.grpc.stub.StreamObserver<io.tinykv.proto.raft.AppendEntriesResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getHandleRequestVoteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.tinykv.proto.raft.RequestVoteRequest,
              io.tinykv.proto.raft.RequestVoteResponse>(
                service, METHODID_HANDLE_REQUEST_VOTE)))
        .addMethod(
          getHandleAppendEntriesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.tinykv.proto.raft.AppendEntriesRequest,
              io.tinykv.proto.raft.AppendEntriesResponse>(
                service, METHODID_HANDLE_APPEND_ENTRIES)))
        .build();
  }

  private static abstract class RaftRpcServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RaftRpcServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.tinykv.proto.raft.RaftProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RaftRpcService");
    }
  }

  private static final class RaftRpcServiceFileDescriptorSupplier
      extends RaftRpcServiceBaseDescriptorSupplier {
    RaftRpcServiceFileDescriptorSupplier() {}
  }

  private static final class RaftRpcServiceMethodDescriptorSupplier
      extends RaftRpcServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RaftRpcServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RaftRpcServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RaftRpcServiceFileDescriptorSupplier())
              .addMethod(getHandleRequestVoteMethod())
              .addMethod(getHandleAppendEntriesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
