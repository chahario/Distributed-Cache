package org.cache;

import org.cache.cluster.ClusterConfig;
import org.cache.core.EvictionPolicy;

/**
 * Immutable configuration for a CacheNode.
 *
 * WHY A SEPARATE CONFIG CLASS?
 * ─────────────────────────────────────────────────────────────────
 * Without config class, CacheNode constructor becomes:
 *   new CacheNode(7001, 100000, EvictionPolicy.LRU, 1000, true, ...)
 *   → hard to read, easy to swap argument order, no defaults
 *
 * With config class:
 *   CacheNode.Config config = new CacheNode.Config.Builder()
 *       .port(7001)
 *       .capacity(100_000)
 *       .build();
 *   → readable, self-documenting, safe defaults
 *
 * Builder pattern: construct complex objects step by step.
 * Each setter returns 'this' → method chaining.
 * build() validates and creates immutable Config.
 */
public class CacheConfig {

    // ─────────────────────────────────────────────────────────────
    // Fields — all final (immutable after construction)
    // ─────────────────────────────────────────────────────────────

    /**
     * TCP port the NIOServer listens on.
     * Default: 7001 (Redis uses 6379, we use 7001 to avoid conflict)
     */
    private final int port;

    /**
     * Total maximum entries across all 32 partitions.
     * Each partition holds capacity/32 entries.
     * Default: 100,000 entries
     */
    private final int capacity;

    /**
     * LRU or LFU eviction policy.
     * Default: LRU (simpler, more predictable for most workloads)
     */
    private final EvictionPolicy evictionPolicy;

    /**
     * How often TTLReaper scans for expired entries (milliseconds).
     * Default: 1000ms (1 second)
     * Lower = faster cleanup, higher CPU usage.
     * Higher = slower cleanup, lower CPU usage.
     */
    private final long reaperIntervalMs;

    private final ClusterConfig clusterConfig;   // null = single-node mode

    // ─────────────────────────────────────────────────────────────
    // Private constructor — use Builder
    // ─────────────────────────────────────────────────────────────

    private CacheConfig(Builder builder) {
        this.port             = builder.port;
        this.capacity         = builder.capacity;
        this.evictionPolicy   = builder.evictionPolicy;
        this.reaperIntervalMs = builder.reaperIntervalMs;
        this.clusterConfig = builder.clusterConfig;
    }

    // ─────────────────────────────────────────────────────────────
    // Getters
    // ─────────────────────────────────────────────────────────────

    public int            getPort()             { return port; }
    public int            getCapacity()         { return capacity; }
    public EvictionPolicy getEvictionPolicy()   { return evictionPolicy; }
    public long           getReaperIntervalMs() { return reaperIntervalMs; }
    public ClusterConfig getClusterConfig() { return clusterConfig; }

    @Override
    public String toString() {
        return "CacheConfig{" +
                "port=" + port +
                ", capacity=" + capacity +
                ", evictionPolicy=" + evictionPolicy +
                ", reaperIntervalMs=" + reaperIntervalMs +
                ", cluster=" + (clusterConfig != null
                ? clusterConfig.toString()
                : "single-node") +
                '}';
    }

    // ─────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────

    /**
     * Builder for CacheConfig.
     *
     * Usage:
     *   CacheConfig config = new CacheConfig.Builder()
     *       .port(7001)
     *       .capacity(100_000)
     *       .evictionPolicy(EvictionPolicy.LRU)
     *       .reaperIntervalMs(1000)
     *       .build();
     *
     * Or with all defaults:
     *   CacheConfig config = new CacheConfig.Builder().build();
     */
    public static class Builder {

        // Default values
        private int            port             = 7001;
        private int            capacity         = 100_000;
        private EvictionPolicy evictionPolicy   = EvictionPolicy.LRU;
        private long           reaperIntervalMs = 1000;
        // Add to Builder
        private ClusterConfig clusterConfig = null;

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder evictionPolicy(EvictionPolicy evictionPolicy) {
            this.evictionPolicy = evictionPolicy;
            return this;
        }

        public Builder reaperIntervalMs(long reaperIntervalMs) {
            this.reaperIntervalMs = reaperIntervalMs;
            return this;
        }
        public Builder cluster(ClusterConfig clusterConfig) {
            this.clusterConfig = clusterConfig;
            return this;
        }

        /**
         * Validates and builds the config.
         *
         * Why validate here and not in CacheConfig constructor?
         *   Builder accumulates state step by step.
         *   build() is the single point where the complete
         *   config is assembled — natural place to validate.
         */
        public CacheConfig build() {
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException(
                        "Port must be between 1 and 65535. Got: " + port
                );
            }
            if (capacity < 32) {
                throw new IllegalArgumentException(
                        "Capacity must be >= 32 (at least 1 per partition). Got: " + capacity
                );
            }
            if (evictionPolicy == null) {
                throw new IllegalArgumentException(
                        "EvictionPolicy cannot be null"
                );
            }
            if (reaperIntervalMs <= 0) {
                throw new IllegalArgumentException(
                        "reaperIntervalMs must be positive. Got: " + reaperIntervalMs
                );
            }
            return new CacheConfig(this);
        }
    }
}