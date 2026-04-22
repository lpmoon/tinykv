package io.tinykv.client;

import java.util.Scanner;

/**
 * Command-line client for TinyKV.
 *
 * Usage:
 *   java -jar tinykv.jar --client --coordinator localhost:8000 --cluster-name my-cluster
 *   java -jar tinykv.jar --client --seed-addresses localhost:7000,localhost:7001
 *
 * Interactive commands:
 *   put <key> <value>   - Put a key-value pair
 *   get <key>           - Get a value by key
 *   delete <key>        - Delete a key
 *   scan <start> <end>  - Scan keys in range
 *   info                - Show cluster info
 *   exit                - Exit the client
 */
public class ClientCMD {

    public static void main(String[] args) throws Exception {
        String coordinatorAddress = null;
        String clusterName = "default";
        java.util.List<String> seedAddresses = new java.util.ArrayList<>();

        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--coordinator" -> coordinatorAddress = args[++i];
                case "--cluster-name" -> clusterName = args[++i];
                case "--seed-addresses" -> {
                    String[] addrs = args[++i].split(",");
                    for (String addr : addrs) {
                        seedAddresses.add(addr.trim());
                    }
                }
                case "--help" -> {
                    printUsage();
                    return;
                }
            }
        }

        if (coordinatorAddress == null && seedAddresses.isEmpty()) {
            System.err.println("Error: must specify either --coordinator or --seed-addresses");
            printUsage();
            System.exit(1);
        }

        System.out.println("TinyKV Client");
        System.out.println("=============");

        TinyKVClient client;
        if (coordinatorAddress != null) {
            System.out.println("Coordinator: " + coordinatorAddress);
            System.out.println("Cluster: " + clusterName);
            client = new TinyKVClient(coordinatorAddress, clusterName);
        } else {
            System.out.println("Seed addresses: " + seedAddresses);
            client = new TinyKVClient(seedAddresses);
        }

        System.out.println();

        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            System.out.print("tinykv> ");
            String line = scanner.nextLine().trim();

            if (line.isEmpty()) {
                continue;
            }

            String[] parts = line.split("\\s+", 3);
            String command = parts[0].toLowerCase();

            try {
                switch (command) {
                    case "put" -> {
                        if (parts.length < 3) {
                            System.out.println("Usage: put <key> <value>");
                        } else {
                            client.put(parts[1].getBytes(), parts[2].getBytes());
                            System.out.println("OK");
                        }
                    }
                    case "get" -> {
                        if (parts.length < 2) {
                            System.out.println("Usage: get <key>");
                        } else {
                            var value = client.get(parts[1].getBytes());
                            if (value.isPresent()) {
                                System.out.println("value: " + new String(value.get()));
                            } else {
                                System.out.println("key not found");
                            }
                        }
                    }
                    case "delete" -> {
                        if (parts.length < 2) {
                            System.out.println("Usage: delete <key>");
                        } else {
                            client.delete(parts[1].getBytes());
                            System.out.println("OK");
                        }
                    }
                    case "scan" -> {
                        if (parts.length < 3) {
                            System.out.println("Usage: scan <start_key> <end_key>");
                        } else {
                            var results = client.scan(parts[1].getBytes(), parts[2].getBytes());
                            System.out.println("Found " + results.size() + " entries:");
                            for (var entry : results) {
                                System.out.println("  " + new String(entry.key()) + " -> " + new String(entry.value()));
                            }
                        }
                    }
                    case "info" -> {
                        var info = client.getClusterInfo();
                        System.out.println("Cluster: " + info.leaderAddress());
                        System.out.println("Leader ID: " + info.leaderId());
                        System.out.println("Nodes: " + info.nodes().size());
                        for (var node : info.nodes()) {
                            System.out.println("  Node " + node.getNodeId() + ": " + node.getAddress() +
                                    (node.getIsLeader() ? " (LEADER)" : ""));
                        }
                    }
                    case "exit", "quit" -> {
                        running = false;
                    }
                    case "help" -> {
                        printCommands();
                    }
                    default -> {
                        System.out.println("Unknown command: " + command);
                        printCommands();
                    }
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }

        client.close();
        System.out.println("Goodbye!");
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar tinykv.jar --client [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --coordinator <addr>   Coordinator address (e.g., localhost:8000)");
        System.out.println("  --cluster-name <name>  Cluster name (default: default)");
        System.out.println("  --seed-addresses <list> Comma-separated list of seed node addresses");
        System.out.println("  --help                  Show this help message");
        System.out.println();
        System.out.println("Interactive commands:");
        printCommands();
    }

    private static void printCommands() {
        System.out.println("  put <key> <value>   Put a key-value pair");
        System.out.println("  get <key>           Get a value by key");
        System.out.println("  delete <key>        Delete a key");
        System.out.println("  scan <start> <end>  Scan keys in range");
        System.out.println("  info                Show cluster info");
        System.out.println("  exit                Exit the client");
    }
}
