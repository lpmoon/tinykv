package io.tinykv.coordinator;

/**
 * Main entry point for the Coordinator service.
 *
 * Usage:
 *   java -jar tinykv.jar --coordinator --coordinator-port 8000
 *
 * Console (HTTP):
 *   http://localhost:8001
 */
public class CoordinatorMain {

    private static final int CONSOLE_PORT = 8001;

    public static void main(String[] args) throws Exception {
        String bindAddress = "localhost";
        int port = 8000;
        long heartbeatTimeoutMs = 10000; // 10 seconds

        // Parse command-line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--coordinator-port" -> port = Integer.parseInt(args[++i]);
                case "--bind-address" -> bindAddress = args[++i];
                case "--heartbeat-timeout" -> heartbeatTimeoutMs = Long.parseLong(args[++i]);
            }
        }

        System.out.println("Starting TinyKV Coordinator...");
        System.out.println("  gRPC Port: " + port);
        System.out.println("  Console:   http://localhost:" + CONSOLE_PORT);
        System.out.println("  Heartbeat timeout: " + heartbeatTimeoutMs + "ms");

        CoordinatorService coordinatorService = new CoordinatorService(bindAddress, port, heartbeatTimeoutMs);
        CoordinatorGrpcServer grpcServer = new CoordinatorGrpcServer(coordinatorService, bindAddress, port);

        // Start gRPC server
        grpcServer.start();

        // Start HTTP console
        ConsoleServer consoleServer = new ConsoleServer(coordinatorService);
        consoleServer.start();

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Coordinator...");
            consoleServer.stop();
            grpcServer.stop();
        }));

        // Block main thread
        Thread.currentThread().join();
    }
}
