package org.cache;

import org.cache.cluster.ClusterRouter;
import org.cache.core.StripedCache;
import org.cache.expiry.TTLReaper;
import org.cache.server.NIOServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The main node of the distributed cache.
 *
 * CacheNode is the "restaurant manager" — it owns and coordinates
 * all components:
 *
 *   StripedCache  = the kitchen     (stores and serves data locally)
 *   TTLReaper     = the cleaner     (removes expired entries)
 *   ClusterRouter = the dispatcher  (decides local vs remote per key)
 *   NIOServer     = the front door  (accepts TCP connections)
 *
 * LIFECYCLE:
 * ─────────────────────────────────────────────────────────────────
 *   start()  → creates components → starts them in dependency order
 *   stop()   → stops them in reverse order → cleans up
 *
 * Startup order and why it matters:
 *   1. StripedCache   — everything else needs it to exist
 *   2. TTLReaper      — needs the cache to scan
 *   3. ClusterRouter  — needs the cache for local hits (optional)
 *   4. NIOServer      — needs cache and router to serve requests
 *   5. Shutdown hook  — needs all components to exist to stop them
 *
 * Shutdown order (reverse):
 *   1. NIOServer      — stop accepting requests FIRST
 *   2. ClusterRouter  — close outbound connections after inbound stops
 *   3. TTLReaper      — stop the background thread
 *   4. Cache          — no explicit stop, GC reclaims; log final stats
 *
 * If we stopped the cache while NIOServer was still accepting:
 * a request would arrive → cache.get() → NullPointerException.
 *
 * SINGLE-NODE VS CLUSTER MODE:
 * ─────────────────────────────────────────────────────────────────
 * Determined by whether CacheConfig carries a ClusterConfig.
 *
 *   null     → single-node. NIOServer talks to StripedCache directly.
 *              No ring, no router, no forwarding overhead.
 *
 *   present  → cluster. NIOServer talks to ClusterRouter, which asks
 *              the consistent hash ring who owns each key and either
 *              serves locally or forwards over TCP.
 *
 * SHUTDOWN HOOK:
 * ─────────────────────────────────────────────────────────────────
 * A thread registered with the JVM, run automatically on:
 *   Ctrl+C, SIGTERM, System.exit(), or when the last non-daemon
 *   thread finishes.
 *
 * Without it, Ctrl+C kills the JVM instantly:
 *   TTLReaper dies mid-scan
 *   NIOServer sockets are never closed → OS holds the port in
 *   TIME_WAIT for ~60s → next startup fails with
 *   "Address already in use"
 *   ClusterRouter's pooled connections leak
 *
 * THREAD SAFETY:
 * ─────────────────────────────────────────────────────────────────
 * start() and stop() are not designed for concurrent invocation.
 * In practice: start() once, stop() once (possibly from the hook).
 * The volatile running flag makes state visible across threads.
 */
public class CacheNode {

    private static final Logger log = LoggerFactory.getLogger(CacheNode.class);

    // ─────────────────────────────────────────────────────────────
    // Components
    // ─────────────────────────────────────────────────────────────

    private final CacheConfig config;

    /**
     * The local cache — 32 striped partitions holding this node's share
     * of the keyspace.
     *
     * In cluster mode this holds ONLY the keys this node owns according
     * to the hash ring. Keys owned by other nodes live on those nodes.
     *
     * null before start() and after stop().
     */
    private StripedCache<String, byte[]> cache;

    /**
     * Background daemon thread — scans all partitions every
     * reaperIntervalMs and removes expired entries.
     *
     * Complements the lazy expiration in CachePartition.get():
     * lazy expiry never fires for keys nobody reads, so without the
     * reaper those entries occupy memory until capacity pressure
     * evicts them.
     *
     * null before start() and after stop().
     */
    private TTLReaper reaper;

    /**
     * Routes operations to the node that owns each key.
     *
     * null in single-node mode (no ClusterConfig supplied) — NIOServer
     * then talks to the local StripedCache directly, avoiding the cost
     * of building a hash ring for a cluster of one.
     *
     * When present, NIOServer talks to the router instead, which asks
     * the ring who owns the key and either serves locally or forwards
     * to the owning node over TCP.
     *
     * null before start() and after stop().
     */
    private ClusterRouter router;

    /**
     * TCP server — one NIO selector thread handling all connections.
     *
     * Receives the router (possibly null) so it can route in cluster
     * mode without knowing anything about rings or forwarding.
     *
     * null before start() and after stop().
     */
    private NIOServer server;

    /**
     * Whether this node is currently running.
     *
     * volatile: written by stop() (possibly from the shutdown hook
     * thread), read by isRunning() and ensureRunning() from any thread.
     * volatile gives visibility without the cost of a lock.
     *
     * Not AtomicBoolean — we need visibility, not compare-and-swap.
     */
    private volatile boolean running = false;

    // ─────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────

    /**
     * Creates a CacheNode. Does NOT start anything.
     *
     * Why separate construction from start()?
     *   A constructor that spawns background threads and binds ports
     *   is a surprise. Construction should be cheap and side-effect
     *   free — you can build a node, inspect its config, and decide
     *   later whether to start it.
     *
     * @param config must be non-null
     */
    public CacheNode(CacheConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("CacheConfig cannot be null");
        }
        this.config = config;
    }

    /**
     * Convenience constructor with all defaults:
     * port=7001, capacity=100_000, LRU, reaper=1000ms, single-node.
     */
    public CacheNode() {
        this(new CacheConfig.Builder().build());
    }

    // ─────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────

    /**
     * Starts the cache node.
     *
     * The running flag is set to true only AFTER every component has
     * started successfully. If NIOServer fails to bind the port, we
     * unwind what we already started and rethrow — running stays false
     * so isRunning() never lies to callers.
     *
     * @throws IllegalStateException if already running
     * @throws RuntimeException      if the server cannot bind the port
     */
    public void start() {
        if (running) {
            throw new IllegalStateException(
                    "CacheNode is already running. Call stop() first."
            );
        }

        log.info("CacheNode starting with config: {}", config);

        // ── Step 1: Create StripedCache ───────────────────────────
        log.info("Creating StripedCache: capacity={} policy={}",
                config.getCapacity(), config.getEvictionPolicy());

        cache = new StripedCache<>(
                config.getCapacity(),
                config.getEvictionPolicy()
        );

        log.info("StripedCache created: 32 partitions, ~{} entries each",
                config.getCapacity() / 32);

        // ── Step 2: Create and start TTLReaper ────────────────────
        log.info("Starting TTLReaper: interval={}ms",
                config.getReaperIntervalMs());

        reaper = new TTLReaper(cache, config.getReaperIntervalMs());
        reaper.start();

        log.info("TTLReaper started");

        // ── Step 3: Create ClusterRouter (cluster mode only) ─────
        // Created BEFORE the server, because the server needs it.
        // Left null in single-node mode — NIOServer handles null.
        if (config.getClusterConfig() != null) {
            log.info("Cluster mode enabled: {}", config.getClusterConfig());

            router = new ClusterRouter(config.getClusterConfig(), cache);

            log.info("ClusterRouter ready: {} ring positions across {} nodes",
                    router.getRing().ringSize(),
                    router.getRing().nodeCount());
        } else {
            log.info("Single-node mode — no cluster routing");
        }

        // ── Step 4: Create and start NIOServer ────────────────────
        log.info("Starting NIOServer on port {}", config.getPort());

        // router may be null — NIOServer falls back to the local cache
        server = new NIOServer(cache, router, config.getPort());

        try {
            server.start();
        } catch (IOException e) {
            // Unwind everything we already started, in reverse order,
            // before rethrowing. Otherwise a failed start leaks a
            // running reaper thread and open pooled connections.
            if (router != null) router.close();
            reaper.stop();

            throw new RuntimeException(
                    "Failed to start NIOServer on port " + config.getPort() +
                            ". Is the port already in use?", e
            );
        }

        log.info("NIOServer started on port {}", config.getPort());

        // ── Step 5: Register shutdown hook ────────────────────────
        registerShutdownHook();

        // Only now is the node genuinely usable
        running = true;

        log.info("CacheNode started successfully");
        logStats();
    }

    /**
     * Stops the cache node gracefully.
     *
     * Idempotent — calling stop() twice is safe. This matters because
     * the shutdown hook fires on JVM exit even if you already called
     * stop() manually.
     */
    public void stop() {
        if (!running) {
            log.warn("CacheNode.stop() called but node is not running — ignoring");
            return;
        }

        log.info("CacheNode stopping...");

        // ── Step 1: Stop NIOServer ────────────────────────────────
        // First, so no new requests can arrive while we tear down
        // the components those requests would depend on.
        if (server != null) {
            log.info("Stopping NIOServer...");
            server.stop();
            log.info("NIOServer stopped");
        }

        // ── Step 2: Close ClusterRouter connections ───────────────
        // After the server, because a request already in flight may
        // still be forwarding to a remote node. Closing the pool
        // mid-forward would throw IOException on a live request.
        if (router != null) {
            log.info("Closing ClusterRouter ({} pooled connections)...",
                    router.openConnections());
            router.close();
            log.info("ClusterRouter closed");
        }

        // ── Step 3: Stop TTLReaper ────────────────────────────────
        if (reaper != null) {
            log.info("Stopping TTLReaper...");
            reaper.stop();
            log.info("TTLReaper stopped: cycles={} totalExpired={}",
                    reaper.getReapCycles(), reaper.getTotalExpired());
        }

        // ── Step 4: Cache — no explicit stop needed ───────────────
        // StripedCache holds no threads or OS resources.
        // GC reclaims the memory once the reference is dropped.
        // Log the final size before that happens.
        if (cache != null) {
            log.info("Final cache size: {}/{}",
                    cache.totalSize(), config.getCapacity());
        }

        running = false;

        log.info("CacheNode stopped cleanly");
    }

    // ─────────────────────────────────────────────────────────────
    // Public cache API
    // ─────────────────────────────────────────────────────────────
    //
    // These are for embedded/in-process use — your Java application
    // holding a CacheNode directly rather than connecting over TCP.
    //
    // IMPORTANT: in cluster mode these go through the router, exactly
    // like a request arriving over TCP would. A key owned by another
    // node is forwarded, not served as a local miss.
    //
    // If these bypassed the router and hit the cache directly, an
    // embedded caller would get null for keys that exist elsewhere
    // in the cluster — a silently wrong answer.
    // ─────────────────────────────────────────────────────────────

    /**
     * Stores a key-value pair with no expiration.
     *
     * @throws IllegalStateException if the node is not running
     * @throws RuntimeException      if the owning node is unreachable
     *                               (cluster mode only)
     */
    public void put(String key, byte[] value) {
        put(key, value, 0);
    }

    /**
     * Stores a key-value pair with a TTL.
     *
     * @param ttlMs expiration in milliseconds, 0 = no expiration
     * @throws IllegalStateException if the node is not running
     * @throws RuntimeException      if the owning node is unreachable
     */
    public void put(String key, byte[] value, long ttlMs) {
        ensureRunning();
        try {
            if (router != null) {
                router.put(key, value, ttlMs);
            } else {
                cache.put(key, value, ttlMs);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to forward put for key '" + key + "' to owning node", e
            );
        }
    }

    /**
     * Retrieves the value for a key.
     *
     * Returns null on cache miss, expiry, or deletion.
     * Throws (rather than returning null) if a remote node is
     * unreachable — "we could not determine whether this key exists"
     * is not the same as "this key does not exist".
     *
     * @throws IllegalStateException if the node is not running
     * @throws RuntimeException      if the owning node is unreachable
     */
    public byte[] get(String key) {
        ensureRunning();
        try {
            return (router != null) ? router.get(key) : cache.get(key);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to forward get for key '" + key + "' to owning node", e
            );
        }
    }

    /**
     * Deletes a key.
     *
     * @return true if the key existed and was removed
     * @throws IllegalStateException if the node is not running
     * @throws RuntimeException      if the owning node is unreachable
     */
    public boolean delete(String key) {
        ensureRunning();
        try {
            return (router != null) ? router.delete(key) : cache.delete(key);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to forward delete for key '" + key + "' to owning node", e
            );
        }
    }

    /**
     * Returns true if the key exists and has not expired.
     *
     * Side effect: updates eviction ordering, exactly like get().
     */
    public boolean containsKey(String key) {
        return get(key) != null;
    }

    /**
     * Returns the number of entries stored on THIS node.
     *
     * In cluster mode this is this node's share of the keyspace,
     * not the cluster-wide total. Summing size() across all nodes
     * gives the cluster total.
     *
     * Approximate — not atomic across the 32 partitions. Suitable
     * for monitoring, not for capacity decisions.
     */
    public int size() {
        ensureRunning();
        return cache.totalSize();
    }

    // ─────────────────────────────────────────────────────────────
    // Monitoring
    // ─────────────────────────────────────────────────────────────

    public boolean isRunning()       { return running; }
    public CacheConfig getConfig()   { return config; }

    /**
     * Returns the cluster router, or null in single-node mode.
     * Useful in tests to inspect key ownership:
     *   node.getRouter().getOwner("user:1")
     */
    public ClusterRouter getRouter() { return router; }

    /**
     * True if this node is part of a cluster.
     */
    public boolean isClustered()     { return router != null; }

    /**
     * Logs current statistics.
     */
    public void logStats() {
        if (cache == null) return;

        log.info(
                "CacheNode stats: localSize={}/{} port={} policy={} mode={} " +
                        "reaperCycles={} totalExpired={}",
                cache.totalSize(),
                config.getCapacity(),
                config.getPort(),
                config.getEvictionPolicy(),
                isClustered() ? "cluster" : "single-node",
                reaper != null ? reaper.getReapCycles()   : 0,
                reaper != null ? reaper.getTotalExpired() : 0
        );
    }

    // ─────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────

    /**
     * Registers a JVM shutdown hook that calls stop().
     *
     * Named thread: "cache-shutdown-hook" is findable in a thread
     * dump. The default "Thread-0" tells you nothing.
     *
     * Double-stop is safe — stop() checks the running flag and
     * returns immediately if already stopped.
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    log.info("Shutdown hook triggered — stopping CacheNode...");
                    stop();
                }, "cache-shutdown-hook")
        );
        log.info("Shutdown hook registered");
    }

    /**
     * Guards every public cache operation.
     *
     * Why throw rather than return null?
     *   A null from get() means "cache miss". If a stopped node also
     *   returned null, the caller could not distinguish "this key
     *   doesn't exist" from "the cache isn't running". Throwing makes
     *   the real problem immediately visible instead of silently
     *   corrupting the caller's logic.
     */
    private void ensureRunning() {
        if (!running) {
            throw new IllegalStateException(
                    "CacheNode is not running. Call start() first."
            );
        }
    }
}