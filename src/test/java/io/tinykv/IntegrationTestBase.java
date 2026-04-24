package io.tinykv;

import io.tinykv.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Base class for integration tests with automatic data cleanup.
 */
public abstract class IntegrationTestBase {

    protected static final String BASE_TEST_DIR = "target/tinykv-test";

    protected String testDir;

    @BeforeEach
    void setupTestDir() throws IOException {
        String testName = getClass().getSimpleName() + "-" + System.currentTimeMillis() + "-" +
                ThreadLocalRandom.current().nextInt(10000);
        testDir = BASE_TEST_DIR + "/" + testName;
        cleanupDirectory(testDir);
        Files.createDirectories(Paths.get(testDir));
    }

    @AfterEach
    void cleanupTestDir() throws IOException {
        cleanupDirectory(testDir);
    }

    /**
     * Recursively delete a directory.
     */
    protected void cleanupDirectory(String dir) throws IOException {
        Path path = Paths.get(dir);
        if (Files.exists(path)) {
            try (var stream = Files.walk(path)) {
                stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    /**
     * Create a config with the test directory.
     */
    protected Config createConfig(String subDir) {
        return new Config().setDataDir(testDir + "/" + subDir);
    }

    /**
     * Sleep for a short time (useful for waiting for async operations).
     */
    protected void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
