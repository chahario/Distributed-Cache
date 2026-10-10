package org.cache;

import org.cache.core.EvictionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the cache server.
 *
 * Usage:
 *   java -jar cache.jar                    → default config
 *   java -jar cache.jar 7001 100000 LRU    → custom config
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting distributed cache...");

        // Build config from args or use defaults
        CacheConfig config = buildConfig(args);

        // Create and start the node
        CacheNode node = new CacheNode(config);
        node.start();

        log.info("Cache is running. Press Ctrl+C to stop.");

        // Keep main thread alive
        // Shutdown hook handles cleanup on Ctrl+C
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            log.info("Main thread interrupted — shutting down");
            node.stop();
        }
    }

    private static CacheConfig buildConfig(String[] args) {
        CacheConfig.Builder builder = new CacheConfig.Builder();

        try {
            if (args.length >= 1) builder.port(Integer.parseInt(args[0]));
            if (args.length >= 2) builder.capacity(Integer.parseInt(args[1]));
            if (args.length >= 3) builder.evictionPolicy(
                    EvictionPolicy.valueOf(args[2].toUpperCase())
            );
        } catch (Exception e) {
            log.warn("Invalid args — using defaults. Error: {}", e.getMessage());
        }

        return builder.build();
    }
}