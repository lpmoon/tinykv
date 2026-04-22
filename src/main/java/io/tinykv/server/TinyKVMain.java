package io.tinykv.server;

import io.tinykv.client.ClientCMD;
import io.tinykv.common.Config;

/**
 * TinyKV main entry point.
 *
 * Usage:
 *   Coordinator:  java -jar tinykv.jar --coordinator --coordinator-port 8000
 *   Single node:  java -jar tinykv.jar --address localhost:7000
 *   Cluster:      java -jar tinykv.jar --address localhost:7000 --peer-addresses "1:localhost:7001,2:localhost:7002,3:localhost:7003"
 *   Client CMD:  java -jar tinykv.jar --client --coordinator localhost:8000 --cluster-name my-cluster
 */
public class TinyKVMain {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("--client")) {
            // Run as client
            String[] clientArgs = new String[args.length - 1];
            System.arraycopy(args, 1, clientArgs, 0, clientArgs.length);
            ClientCMD.main(clientArgs);
            return;
        }

        if (args.length > 0 && args[0].equals("--coordinator")) {
            // Run as coordinator
            io.tinykv.coordinator.CoordinatorMain.main(args);
            return;
        }

        // Run as server
        Config config = new Config();

        // Parse command-line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--data-dir" -> config.setDataDir(args[++i]);
                case "--port" -> config.setPort(Integer.parseInt(args[++i]));
                case "--address" -> config.setAddress(args[++i]);
                case "--peer-addresses" -> config.setPeerAddresses(args[++i]);
                case "--cluster-name" -> config.setClusterName(args[++i]);
                case "--coordinator" -> config.setCoordinatorAddress(args[++i]);
                case "--peers" -> {
                    // Legacy format: "1:host:port,2:host:port,..."
                    String peers = args[++i];
                    parseLegacyPeers(config, peers);
                }
                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    System.exit(1);
                }
            }
        }

        // Use port as fallback for address
        if (config.getAddress() == null || config.getAddress().isEmpty()) {
            config.setAddress("localhost:" + config.getPort());
        }

        System.out.println("Starting TinyKV...");
        System.out.println("  Address: " + config.getAddress());
        System.out.println("  Peer addresses: " + config.getPeerAddresses());
        System.out.println("  Cluster name: " + config.getClusterName());
        System.out.println("  Coordinator: " + config.getCoordinatorAddress());
        System.out.println("  Data dir: " + config.getDataDir());

        TinyKVService service = new TinyKVService(config);
        service.start();

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down TinyKV...");
            service.stop();
        }));

        // Block main thread
        Thread.currentThread().join();
    }

    private static void parseLegacyPeers(Config config, String peers) {
        // Legacy format: "1:host:port,2:host:port,..."
        // This sets both --address and --peer-addresses
        StringBuilder peerAddresses = new StringBuilder();
        String[] parts = peers.split(",");
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                peerAddresses.append(",");
            }
            peerAddresses.append(parts[i]);
        }
        config.setPeerAddresses(peerAddresses.toString());

        // First entry is our own address
        if (parts.length > 0) {
            String[] addrParts = parts[0].trim().split(":");
            if (addrParts.length >= 3) {
                config.setAddress(addrParts[1] + ":" + addrParts[2]);
            }
        }
    }
}
