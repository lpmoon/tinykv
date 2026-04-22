package io.tinykv.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * In-process transport for testing. Routes messages between RaftNodes
 * in the same JVM.
 */
public class LocalTransport implements RaftTransport {

    private static final Logger LOG = LoggerFactory.getLogger(LocalTransport.class);

    private final Map<Integer, RaftNode> nodes = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "raft-transport");
        t.setDaemon(true);
        return t;
    });

    @Override
    public void sendAsync(RaftMessage message, RaftMessageCallback callback) {
        executor.submit(() -> {
            RaftNode target = nodes.get(message.to());
            if (target == null) {
                LOG.warn("No node {} to send message to", message.to());
                return;
            }

            RaftMessage response = dispatch(target, message);
            if (response != null && callback != null) {
                callback.onResponse(response);
            }
        });
    }

    @Override
    public RaftMessage sendSync(RaftMessage message, long timeoutMs) {
        Future<RaftMessage> future = executor.submit(() -> {
            RaftNode target = nodes.get(message.to());
            if (target == null) return null;
            return dispatch(target, message);
        });

        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            LOG.error("Sync send failed", e);
            return null;
        }
    }

    private RaftMessage dispatch(RaftNode target, RaftMessage message) {
        if (message instanceof RaftMessage.RequestVote req) {
            return target.handleRequestVote(req);
        } else if (message instanceof RaftMessage.AppendEntries req) {
            return target.handleAppendEntries(req);
        } else {
            LOG.warn("Unknown message type: {}", message.getClass());
            return null;
        }
    }

    @Override
    public void register(int nodeId, RaftNode node) {
        nodes.put(nodeId, node);
    }

    @Override
    public void start() {
        LOG.info("Local transport started with {} nodes", nodes.size());
    }

    @Override
    public void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
