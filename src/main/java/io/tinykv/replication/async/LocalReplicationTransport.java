package io.tinykv.replication.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * In-process transport for async replication testing.
 */
public class LocalReplicationTransport implements ReplicationTransport {

    private static final Logger LOG = LoggerFactory.getLogger(LocalReplicationTransport.class);

    private final Map<Integer, AsyncReplicator> replicators = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "async-repl-transport");
        t.setDaemon(true);
        return t;
    });

    @Override
    public void sendAsync(ReplicationMessage message, ReplicationCallback callback) {
        executor.submit(() -> {
            AsyncReplicator target = replicators.get(message.to());
            if (target == null) {
                LOG.warn("No replicator {} to send message to", message.to());
                return;
            }

            ReplicationMessage response = dispatch(target, message);
            if (response != null && callback != null) {
                callback.onResponse(response);
            }
        });
    }

    private ReplicationMessage dispatch(AsyncReplicator target, ReplicationMessage message) {
        if (message instanceof ReplicationMessage.ReplicateEntries req) {
            return target.handleReplicateEntries(req);
        } else if (message instanceof ReplicationMessage.Heartbeat req) {
            return target.handleHeartbeat(req);
        } else {
            LOG.warn("Unknown replication message type: {}", message.getClass());
            return null;
        }
    }

    @Override
    public void register(int nodeId, AsyncReplicator replicator) {
        replicators.put(nodeId, replicator);
    }

    @Override
    public void start() {
        LOG.info("Local replication transport started with {} replicators", replicators.size());
    }

    @Override
    public void stop() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
