package org.cache.cluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Static cluster membership: which nodes exist and how to reach them.
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY STATIC MEMBERSHIP?
 * ─────────────────────────────────────────────────────────────────
 * Production clusters discover members dynamically — via a gossip
 * protocol (Cassandra, Redis Cluster) or a coordination service
 * (ZooKeeper, etcd, Consul). Nodes announce themselves, detect
 * failures with heartbeats, and the ring updates automatically.
 *
 * That is a large subsystem: failure detection, split-brain
 * handling, membership consensus, anti-entropy repair.
 *
 * We use static configuration instead: every node is told the full
 * member list at startup. This is honest and correct for a fixed
 * cluster, and keeps the focus on the routing logic.
 *
 * Interview framing: "I used static membership to keep the scope
 * on consistent hashing and routing. Production would need gossip
 * or a coordination service for dynamic membership and failure
 * detection — that's the natural next layer, and the ClusterRouter
 * would consume membership changes through the same addNode/
 * removeNode API the ring already exposes."
 *
 * ─────────────────────────────────────────────────────────────────
 * NODE IDs
 * ─────────────────────────────────────────────────────────────────
 * A nodeId is "host:port" — e.g. "10.0.1.5:7001".
 *
 * Why that format?
 *   1. Globally unique — no two nodes share a host+port
 *   2. Directly usable as a connection address
 *   3. Deterministic across machines — every node in the cluster
 *      computes the same ring positions from the same id string
 *
 * The id is what gets hashed onto the ring, so it MUST be identical
 * on every machine. "localhost:7001" and "127.0.0.1:7001" would
 * hash to different positions and split the cluster's view of
 * ownership. Always use the same canonical form everywhere.
 */
public class ClusterConfig {

    /**
     * This node's own id — "host:port".
     *
     * Used by ClusterRouter to decide local vs remote:
     *   ring.getNode(key).equals(localNodeId) → serve locally
     *   otherwise                              → forward over TCP
     */
    private final String localNodeId;

    /**
     * All cluster members: nodeId → "host:port".
     *
     * Includes the local node. LinkedHashMap preserves insertion
     * order so logs and diagnostics list nodes consistently.
     */
    private final Map<String, String> nodeAddresses;

    private ClusterConfig(String localNodeId, Map<String, String> nodeAddresses) {
        this.localNodeId   = localNodeId;
        this.nodeAddresses = Collections.unmodifiableMap(new LinkedHashMap<>(nodeAddresses));
    }

    // ─────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────

    public String getLocalNodeId() {
        return localNodeId;
    }

    public Map<String, String> getNodeAddresses() {
        return nodeAddresses;
    }

    /**
     * Resolves a nodeId to its "host:port" address.
     * @return the address, or null if the node is unknown
     */
    public String getAddress(String nodeId) {
        return nodeAddresses.get(nodeId);
    }

    public boolean isLocal(String nodeId) {
        return localNodeId.equals(nodeId);
    }

    public int size() {
        return nodeAddresses.size();
    }

    /** True if this is a single-node "cluster" — no remote forwarding needed. */
    public boolean isSingleNode() {
        return nodeAddresses.size() <= 1;
    }

    @Override
    public String toString() {
        return "ClusterConfig{local=" + localNodeId +
                ", members=" + nodeAddresses.keySet() + "}";
    }

    // ─────────────────────────────────────────────────────────────
    // Builder
    // ─────────────────────────────────────────────────────────────

    /**
     * Usage:
     *   ClusterConfig config = new ClusterConfig.Builder()
     *       .localNode("10.0.1.5", 7001)
     *       .addNode("10.0.1.5", 7001)   // include self
     *       .addNode("10.0.1.6", 7001)
     *       .addNode("10.0.1.7", 7001)
     *       .build();
     */
    public static class Builder {

        private String localNodeId;
        private final Map<String, String> nodes = new LinkedHashMap<>();

        /**
         * Sets this node's identity.
         * Must also be added via addNode() — a node is a member
         * of its own cluster.
         */
        public Builder localNode(String host, int port) {
            this.localNodeId = host + ":" + port;
            return this;
        }

        public Builder localNode(String nodeId) {
            this.localNodeId = nodeId;
            return this;
        }

        /**
         * Adds a cluster member.
         * The nodeId and the address are the same string —
         * "host:port" serves as both identity and connection target.
         */
        public Builder addNode(String host, int port) {
            String nodeId = host + ":" + port;
            nodes.put(nodeId, nodeId);
            return this;
        }

        public Builder addNode(String nodeId) {
            nodes.put(nodeId, nodeId);
            return this;
        }

        /**
         * Validates and builds.
         *
         * Why require localNodeId to be in the member list?
         *   ClusterRouter compares ring.getNode(key) against
         *   localNodeId to decide local vs remote. If this node
         *   is not on the ring, it will NEVER be chosen as an
         *   owner — every single request forwards to a remote node,
         *   including requests that arrived here. Infinite forwarding.
         *   Catching this at construction is far better than
         *   debugging it in production.
         */
        public ClusterConfig build() {
            if (localNodeId == null || localNodeId.isEmpty()) {
                throw new IllegalArgumentException(
                        "localNodeId must be set. Call localNode(host, port)."
                );
            }
            if (nodes.isEmpty()) {
                throw new IllegalArgumentException(
                        "Cluster must contain at least one node. Call addNode()."
                );
            }
            if (!nodes.containsKey(localNodeId)) {
                throw new IllegalArgumentException(
                        "localNodeId '" + localNodeId + "' is not in the member list " +
                                nodes.keySet() + ". This node must be a member of its own " +
                                "cluster, otherwise every request would forward remotely forever."
                );
            }
            return new ClusterConfig(localNodeId, nodes);
        }
    }
}