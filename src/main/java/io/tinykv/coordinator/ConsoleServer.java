package io.tinykv.coordinator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP Console server for the Coordinator.
 *
 * Provides a web UI to monitor:
 * - All registered clusters
 * - Nodes in each cluster
 * - Node role (Leader/Follower)
 * - Node status (Active/Stale)
 *
 * Console is available at: http://localhost:8001
 */
public class ConsoleServer {

    private static final int CONSOLE_PORT = 8001;

    private final CoordinatorService coordinatorService;
    private HttpServer server;

    public ConsoleServer(CoordinatorService coordinatorService) {
        this.coordinatorService = coordinatorService;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(CONSOLE_PORT), 0);
        server.createContext("/", new ConsoleHandler());
        server.setExecutor(null);
        server.start();
        System.out.println("Console available at: http://localhost:" + CONSOLE_PORT);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private class ConsoleHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String html = generateHtml();
            exchange.sendResponseHeaders(200, html.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(html.getBytes());
            }
        }

        private String generateHtml() {
            StringBuilder sb = new StringBuilder();
            sb.append("<!DOCTYPE html>\n");
            sb.append("<html>\n");
            sb.append("<head>\n");
            sb.append("<meta charset=\"UTF-8\">\n");
            sb.append("<title>TinyKV Coordinator Console</title>\n");
            sb.append("<style>\n");
            sb.append("body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background: #f5f5f5; }\n");
            sb.append("h1 { color: #333; }\n");
            sb.append(".cluster { background: white; border-radius: 8px; padding: 20px; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }\n");
            sb.append(".cluster h2 { margin-top: 0; color: #2c3e50; }\n");
            sb.append("table { width: 100%; border-collapse: collapse; margin-top: 15px; }\n");
            sb.append("th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }\n");
            sb.append("th { background: #f8f9fa; font-weight: 600; }\n");
            sb.append(".status-ok { color: #27ae60; font-weight: bold; }\n");
            sb.append(".status-stale { color: #e74c3c; font-weight: bold; }\n");
            sb.append(".role-leader { color: #f39c12; font-weight: bold; }\n");
            sb.append(".role-follower { color: #3498db; }\n");
            sb.append(".timestamp { color: #999; font-size: 12px; }\n");
            sb.append(".no-nodes { color: #999; font-style: italic; }\n");
            sb.append(".refresh { margin: 20px 0; }\n");
            sb.append("</style>\n");
            sb.append("</head>\n");
            sb.append("<body>\n");

            sb.append("<h1>TinyKV Coordinator Console</h1>\n");
            sb.append("<p class='timestamp'>Last updated: ").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("</p>\n");
            sb.append("<div class='refresh'><button onclick='location.reload()'>Refresh</button></div>\n");

            // Get all clusters info
            Map<String, ClusterInfo> clusterInfos = getAllClusterInfo();

            if (clusterInfos.isEmpty()) {
                sb.append("<p class='no-nodes'>No clusters registered yet.</p>\n");
            } else {
                for (Map.Entry<String, ClusterInfo> entry : clusterInfos.entrySet()) {
                    sb.append("<div class='cluster'>\n");
                    sb.append("<h2>Cluster: ").append(escapeHtml(entry.getKey())).append("</h2>\n");

                    ClusterInfo info = entry.getValue();
                    if (info.nodes.isEmpty()) {
                        sb.append("<p class='no-nodes'>No nodes in this cluster.</p>\n");
                    } else {
                        sb.append("<table>\n");
                        sb.append("<tr><th>Node ID</th><th>Address</th><th>Role</th><th>Status</th><th>Last Heartbeat</th></tr>\n");

                        for (NodeStatus node : info.nodes) {
                            sb.append("<tr>\n");
                            sb.append("<td>").append(node.nodeId).append("</td>\n");
                            sb.append("<td>").append(escapeHtml(node.address)).append("</td>\n");

                            // Role
                            sb.append("<td>");
                            if (node.isLeader) {
                                sb.append("<span class='role-leader'>LEADER</span>");
                            } else {
                                sb.append("<span class='role-follower'>FOLLOWER</span>");
                            }
                            sb.append("</td>\n");

                            // Status
                            sb.append("<td>");
                            if (node.isActive) {
                                sb.append("<span class='status-ok'>ACTIVE</span>");
                            } else {
                                sb.append("<span class='status-stale'>STALE</span>");
                            }
                            sb.append("</td>\n");

                            // Last heartbeat
                            sb.append("<td>").append(node.lastHeartbeatStr).append("</td>\n");
                            sb.append("</tr>\n");
                        }

                        sb.append("</table>\n");
                    }
                    sb.append("</div>\n");
                }
            }

            sb.append("<div class='refresh'>\n");
            sb.append("<button onclick='location.reload()'>Refresh</button>\n");
            sb.append("</div>\n");

            sb.append("<hr>\n");
            sb.append("<p class='timestamp'>\n");
            sb.append("Coordinator: ").append(coordinatorService.getBindAddress()).append(":").append(coordinatorService.getPort()).append("<br>\n");
            sb.append("Console: http://localhost:").append(CONSOLE_PORT).append("\n");
            sb.append("</p>\n");

            sb.append("</body>\n");
            sb.append("</html>\n");

            return sb.toString();
        }

        private Map<String, ClusterInfo> getAllClusterInfo() {
            Map<String, ClusterInfo> result = new HashMap<>();

            // We need to access the internal clusters map
            // For now, we'll query each known cluster
            // In a real implementation, we'd expose a method to get all cluster names
            // Using reflection to access the clusters map for simplicity
            try {
                java.lang.reflect.Field clustersField = CoordinatorService.class.getDeclaredField("clusters");
                clustersField.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<String, ?> clusters = (Map<String, ?>) clustersField.get(coordinatorService);

                if (clusters != null) {
                    for (Map.Entry<String, ?> entry : clusters.entrySet()) {
                        String clusterName = entry.getKey();
                        Object clusterState = entry.getValue();

                        // Get all nodes
                        java.lang.reflect.Method getAllNodes = clusterState.getClass().getDeclaredMethod("getAllNodes");
                        @SuppressWarnings("unchecked")
                        List<?> nodes = (List<?>) getAllNodes.invoke(clusterState);

                        // Get leader
                        java.lang.reflect.Method getLeader = clusterState.getClass().getDeclaredMethod("getLeader");
                        Object leader = getLeader.invoke(clusterState);

                        ClusterInfo info = new ClusterInfo();
                        info.nodes = new java.util.ArrayList<>();

                        for (Object nodeObj : nodes) {
                            NodeStatus node = new NodeStatus();
                            node.nodeId = (Integer) nodeObj.getClass().getField("nodeId").get(nodeObj);
                            node.address = (String) nodeObj.getClass().getField("address").get(nodeObj);
                            node.isLeader = (Boolean) nodeObj.getClass().getField("isLeader").get(nodeObj);
                            long lastHeartbeat = (Long) nodeObj.getClass().getField("lastHeartbeat").get(nodeObj);
                            node.isActive = isNodeActive(lastHeartbeat);
                            node.lastHeartbeatStr = formatHeartbeat(lastHeartbeat);
                            info.nodes.add(node);
                        }

                        result.put(clusterName, info);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error getting cluster info: " + e.getMessage());
            }

            return result;
        }

        private boolean isNodeActive(long lastHeartbeat) {
            long now = System.currentTimeMillis();
            // Consider stale if no heartbeat in 30 seconds
            return (now - lastHeartbeat) < 30000;
        }

        private String formatHeartbeat(long timestamp) {
            if (timestamp == 0) return "Never";
            long now = System.currentTimeMillis();
            long diff = now - timestamp;

            if (diff < 1000) {
                return diff + "ms ago";
            } else if (diff < 60000) {
                return (diff / 1000) + "s ago";
            } else {
                return new SimpleDateFormat("HH:mm:ss").format(new Date(timestamp));
            }
        }

        private String escapeHtml(String s) {
            if (s == null) return "";
            return s.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;");
        }
    }

    // Internal types for HTML generation
    private static class ClusterInfo {
        java.util.List<NodeStatus> nodes;
    }

    private static class NodeStatus {
        int nodeId;
        String address;
        boolean isLeader;
        boolean isActive;
        String lastHeartbeatStr;
    }
}
