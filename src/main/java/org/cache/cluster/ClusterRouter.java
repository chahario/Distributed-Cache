package org.cache.cluster;

import org.cache.client.CacheClient;
import org.cache.core.StripedCache;
import org.cache.hash.ConsistentHashRing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes cache operations to the node that owns the key.
 *
 * Sits between NIOServer and StripedCache:
 *
 *   Client request arrives at ANY node
 *          ↓
 *   ClusterRouter asks the ring: who owns this key?
 *          ↓
 *   ┌──────────────────┬──────────────────────────┐
 *   │ This node owns it│ Another node owns it     │
 *   │ serve from local │ forward via CacheClient  │
 *   │ StripedCache     │ over TCP                 │
 *   └──────────────────┴──────────────────────────┘
 *
 * This means a client can connect to ANY node in the cluster and
 * get the right answer. The cluster is transparent to the client.
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY POOL CONNECTIONS TO REMOTE NODES?
 * ─────────────────────────────────────────────────────────────────
 * Opening a TCP connection costs a three-way handshake — one full
 * network round trip, typically 0.5-50ms depending on distance.
 *
 * Without pooling, every forwarded request would pay that cost:
 *   connect (RTT) → send request (RTT) → receive → close
 *   Roughly 3x the latency of the actual cache operation.
 *
 * With pooling, connections are opened once and reused:
 *   send request (RTT) → receive
 *
 * The pool is a ConcurrentHashMap<nodeId, CacheClient>.
 * computeIfAbsent() creates the connection on first use and
 * returns the existing one afterwards.
 *
 * ─────────────────────────────────────────────────────────────────
 * KNOWN LIMITATION — ONE CONNECTION PER REMOTE NODE
 * ─────────────────────────────────────────────────────────────────
 * CacheClient is NOT thread-safe: our protocol correlates requests
 * and responses by ORDER, not by request id. Two threads writing
 * to the same socket would interleave and each could read the
 * other's response.
 *
 * With one pooled client per remote node, concurrent forwards to
 * the same node must be serialized — a bottleneck under load.
 *
 * Real fixes, in increasing order of effort:
 *   1. Pool N connections per node, hand one to each thread
 *      (simplest, standard approach)
 *   2. Add a request-id field to the protocol header — the reserved
 *      byte was left there for exactly this — and multiplex many
 *      in-flight requests over one connection
 *   3. Make the forwarding path fully async with NIO on the client
 *
 * We synchronize on the client here so the code is CORRECT rather
 * than silently corrupting responses. Documented as a known
 * bottleneck rather than hidden.
 *
 * ─────────────────────────────────────────────────────────────────
 * FAILURE HANDLING
 * ─────────────────────────────────────────────────────────────────
 * If a remote node is down, the forward throws IOException.
 * We remove that client from the pool so the next request retries
 * a fresh connection rather than reusing a dead socket forever,
 * then rethrow so the caller knows the request failed.
 *
 * We deliberately do NOT fall back to the local cache on failure.
 * Serving a local miss for a key this node does not own would
 * silently return null for data that actually exists on the failed
 * node — a wrong answer is worse than an error.
 */
public class ClusterRouter {

    private static final Logger log = LoggerFactory.getLogger(ClusterRouter.class);

    private final ClusterConfig        config;
    private final ConsistentHashRing   ring;
    private final StripedCache<String, byte[]> localCache;

    /**
     * Pooled connections to remote nodes: nodeId → CacheClient.
     *
     * ConcurrentHashMap because multiple NIO/worker threads may
     * request a client for the same node simultaneously.
     * computeIfAbsent is atomic — only one connection is created
     * even under concurrent first-use.
     */
    private final Map<String, CacheClient> clientPool = new ConcurrentHashMap<>();

    // ─────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────

    public ClusterRouter(
            ClusterConfig config,
            StripedCache<String, byte[]> localCache) {

        if (config == null)     throw new IllegalArgumentException("config cannot be null");
        if (localCache == null) throw new IllegalArgumentException("localCache cannot be null");

        this.config     = config;
        this.localCache = localCache;
        this.ring       = new ConsistentHashRing();

        // Populate the ring with every cluster member.
        // Every node in the cluster builds an IDENTICAL ring because
        // MurmurHash3 is deterministic and the nodeId strings match.
        for (String nodeId : config.getNodeAddresses().keySet()) {
            ring.addNode(nodeId);
        }

        log.info("ClusterRouter initialized. local={} members={} ringPositions={}",
                config.getLocalNodeId(), config.size(), ring.ringSize());
    }

    // ─────────────────────────────────────────────────────────────
    // Routed operations
    // ─────────────────────────────────────────────────────────────

    /**
     * Retrieves a value, routing to the owning node.
     *
     * @return the value, or null on miss / expiry
     * @throws IOException if the owning node is unreachable
     */
    public byte[] get(String key) throws IOException {
        String owner = ring.getNode(key);

        if (config.isLocal(owner)) {
            return localCache.get(key);
        }

        return forwardGet(owner, key);
    }

    /**
     * Stores a value, routing to the owning node.
     *
     * @param ttlMs expiration in milliseconds, 0 = none
     * @throws IOException if the owning node is unreachable
     */
    public void put(String key, byte[] value, long ttlMs) throws IOException {
        String owner = ring.getNode(key);

        if (config.isLocal(owner)) {
            localCache.put(key, value, ttlMs);
            return;
        }

        forwardPut(owner, key, value, ttlMs);
    }

    /**
     * Deletes a key, routing to the owning node.
     *
     * @return true if the key existed and was removed
     * @throws IOException if the owning node is unreachable
     */
    public boolean delete(String key) throws IOException {
        String owner = ring.getNode(key);

        if (config.isLocal(owner)) {
            return localCache.delete(key);
        }

        return forwardDelete(owner, key);
    }

    // ─────────────────────────────────────────────────────────────
    // Forwarding
    // ─────────────────────────────────────────────────────────────

    private byte[] forwardGet(String nodeId, String key) throws IOException {
        CacheClient client = getOrCreateClient(nodeId);
        try {
            // synchronized: CacheClient is not thread-safe —
            // see the class-level limitation note
            synchronized (client) {
                return client.get(key);
            }
        } catch (IOException e) {
            evictDeadClient(nodeId, e);
            throw e;
        }
    }

    private void forwardPut(String nodeId, String key, byte[] value, long ttlMs)
            throws IOException {

        CacheClient client = getOrCreateClient(nodeId);

        // Protocol TTL field is 2 bytes and carries SECONDS.
        // Convert from the milliseconds our cache API uses,
        // and clamp to the 65535 wire maximum (~18 hours).
        int ttlSeconds = (int) Math.min(ttlMs / 1000, 65535);

        try {
            synchronized (client) {
                client.set(key, value, ttlSeconds);
            }
        } catch (IOException e) {
            evictDeadClient(nodeId, e);
            throw e;
        }
    }

    private boolean forwardDelete(String nodeId, String key) throws IOException {
        CacheClient client = getOrCreateClient(nodeId);
        try {
            synchronized (client) {
                return client.delete(key);
            }
        } catch (IOException e) {
            evictDeadClient(nodeId, e);
            throw e;
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Connection pool
    // ─────────────────────────────────────────────────────────────

    /**
     * Returns a pooled connection to a remote node, creating it
     * on first use.
     *
     * computeIfAbsent is atomic — if two threads request the same
     * node's client simultaneously, only one connection is created.
     *
     * The lambda cannot throw a checked IOException, so we wrap it
     * in RuntimeException inside and unwrap it here. Ugly but
     * unavoidable with computeIfAbsent's signature.
     */
    private CacheClient getOrCreateClient(String nodeId) throws IOException {
        try {
            return clientPool.computeIfAbsent(nodeId, id -> {
                String address = config.getAddress(id);
                if (address == null) {
                    throw new IllegalStateException(
                            "No address configured for node: " + id
                    );
                }

                String[] parts = address.split(":");
                String host = parts[0];
                int    port = Integer.parseInt(parts[1]);

                try {
                    log.info("Opening connection to remote node {}", id);
                    return new CacheClient(host, port);
                } catch (IOException e) {
                    // computeIfAbsent cannot propagate checked exceptions
                    throw new RuntimeException(
                            "Failed to connect to node " + id + " at " + address, e
                    );
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException io) throw io;
            throw e;
        }
    }

    /**
     * Removes a failed client from the pool and closes it.
     *
     * Why remove rather than reuse?
     *   A socket that threw IOException is usually dead — the peer
     *   closed it or the network dropped. Reusing it fails again
     *   on every subsequent request, forever.
     *   Removing it means the next request calls computeIfAbsent,
     *   which opens a fresh connection — automatic recovery once
     *   the remote node comes back.
     */
    private void evictDeadClient(String nodeId, IOException cause) {
        log.warn("Connection to node {} failed — removing from pool: {}",
                nodeId, cause.getMessage());

        CacheClient dead = clientPool.remove(nodeId);
        if (dead != null) {
            try {
                dead.close();
            } catch (IOException e) {
                log.debug("Error closing dead client for {}", nodeId, e);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Lifecycle and inspection
    // ─────────────────────────────────────────────────────────────

    /**
     * Closes all pooled connections.
     * Called by CacheNode.stop().
     */
    public void close() {
        log.info("Closing {} pooled connections", clientPool.size());

        for (Map.Entry<String, CacheClient> entry : clientPool.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.warn("Error closing connection to {}", entry.getKey(), e);
            }
        }
        clientPool.clear();
    }

    /**
     * Returns which node owns a key — useful for debugging
     * and for verifying distribution in tests.
     */
    public String getOwner(String key) {
        return ring.getNode(key);
    }

    public boolean isLocalKey(String key) {
        return config.isLocal(ring.getNode(key));
    }

    public ConsistentHashRing getRing()   { return ring; }
    public ClusterConfig      getConfig() { return config; }
    public int openConnections()          { return clientPool.size(); }
}