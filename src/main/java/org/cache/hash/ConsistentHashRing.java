package org.cache.hash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Consistent hash ring with virtual nodes.
 *
 * Maps keys to nodes such that adding or removing a node moves
 * only ~1/N of keys instead of nearly all of them.
 *
 * ─────────────────────────────────────────────────────────────────
 * THE RING
 * ─────────────────────────────────────────────────────────────────
 * Conceptually a circle spanning the full 64-bit long range.
 * Both nodes and keys are hashed onto positions on this circle.
 *
 * A key belongs to the first node found walking CLOCKWISE
 * from the key's position.
 *
 *          Long.MIN_VALUE ────────► Long.MAX_VALUE ─┐
 *                ▲                                   │
 *                └───────────────────────────────────┘
 *                          (wraps around)
 *
 * TreeMap.ceilingEntry(keyHash) is exactly "walk clockwise":
 *   returns the entry with the smallest key >= keyHash.
 * If none exists (key is past the last node), wrap to firstEntry().
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY VIRTUAL NODES
 * ─────────────────────────────────────────────────────────────────
 * With 3 physical nodes placed once each, their ring positions
 * are random and clumpy:
 *
 *   NodeA at 100, NodeB at 150, NodeC at 8000
 *   NodeB owns arc 101→150   (50 units)     → ~0.5% of keys
 *   NodeC owns arc 151→8000  (7849 units)   → ~78% of keys
 *
 * Placing each node 200 times spreads it into 200 small arcs
 * scattered around the ring. Statistically each node owns ~1/N.
 *
 *   vnodes    distribution deviation
 *   ──────────────────────────────────
 *    10       ~23%   terrible
 *    50       ~10%   poor
 *   100        ~7%   acceptable
 *   200        ~3%   excellent  ← our choice
 *   500        ~2%   diminishing returns
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY MurmurHash AND NOT String.hashCode()
 * ─────────────────────────────────────────────────────────────────
 * hashCode() is a polynomial hash — "NodeA#0", "NodeA#1", "NodeA#2"
 * produce consecutive integers. All 200 vnodes land in one tight
 * cluster, which defeats the entire purpose of virtual nodes.
 *
 * MurmurHash's avalanche property scatters them across the ring.
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY TreeMap AND NOT A SORTED ARRAY
 * ─────────────────────────────────────────────────────────────────
 * Lookup:    TreeMap O(log n), sorted array O(log n)   — tie
 * Insertion: TreeMap O(log n), sorted array O(n)        — TreeMap wins
 *
 * Nodes join and leave during cluster operations. TreeMap handles
 * dynamic membership without shifting every element.
 *
 * ─────────────────────────────────────────────────────────────────
 * THREAD SAFETY
 * ─────────────────────────────────────────────────────────────────
 * addNode() and removeNode() are synchronized — membership changes
 * are rare and must be atomic across both the ring and the
 * node→vnodes index.
 *
 * getNode() is NOT synchronized — it is on the hot path of every
 * cache operation. TreeMap reads during a concurrent structural
 * modification could see an inconsistent state.
 *
 * This is acceptable because:
 *   1. Membership changes happen at cluster startup or during a
 *      node failure — seconds apart, not microseconds
 *   2. A stale read during a change routes to the old node, which
 *      simply returns a cache miss — no corruption
 *
 * For strict correctness under frequent membership changes, replace
 * TreeMap with a copy-on-write snapshot: build a new TreeMap on each
 * change and swap a volatile reference. Reads then always see one
 * consistent snapshot.
 */


public class ConsistentHashRing {
    /**
     * Default virtual nodes per physical node.
     * 200 gives ~3% distribution deviation — the sweet spot
     * between balance quality and ring memory.
     */

    public static final int DEFAULT_VIRTUAL_NODES = 200;

    /**
     * The ring itself: ring position → physical node id.
     *
     * Multiple positions map to the same node id — that is exactly
     * what virtual nodes are. With 3 nodes and 200 vnodes each,
     * this map holds 600 entries pointing at 3 distinct node ids.
     *
     * TreeMap keeps positions sorted so ceilingEntry() works.
     */

    private final TreeMap<Long,String> ring = new TreeMap<>();

    /**
     * Reverse index: physical node id → its vnode positions.
     *
     * Needed for removeNode(). Without it, removing a node would
     * require scanning all 600 ring entries to find which belong
     * to that node — O(n) instead of O(vnodes).
     *
     * Also makes getNodes() trivial — just the key set.
     */
    private final Map<String,List<Long>> nodeToVnodes = new HashMap<>();

    private final int virtualNodes;

    // ─────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────

    public ConsistentHashRing() {
        this(DEFAULT_VIRTUAL_NODES);
    }
    /**
     * @param virtualNodes vnodes per physical node.
     *                     Higher = better balance, more ring memory.
     *                     Must be positive. 200 is the recommended default.
     */
    public ConsistentHashRing(int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException(
                    "virtual nodes must be positive integer. Got " + virtualNodes
            );
        }
        this.virtualNodes = virtualNodes;

    }

    // ─────────────────────────────────────────────────────────────
    // Membership
    // ─────────────────────────────────────────────────────────────

    /**
     * Adds a physical node to the ring.
     *
     * Creates `virtualNodes` positions by hashing "nodeId#0",
     * "nodeId#1", ... "nodeId#199".
     *
     * The "#i" suffix is what produces different positions for the
     * same physical node. MurmurHash's avalanche property makes
     * "NodeA#0" and "NodeA#1" land far apart on the ring.
     *
     * DETERMINISM MATTERS:
     *   Every node in the cluster computes the same ring positions
     *   for the same nodeId. hash64("NodeA#0") is identical on every
     *   machine, so every machine agrees on who owns which key.
     *   This is why we use MurmurHash and not Object.hashCode()
     *   (which is JVM-instance-specific for non-String types).
     *
     * Idempotent: adding an existing node is a no-op rather than
     * duplicating its vnodes.
     *
     * @param nodeId unique identifier, typically "host:port"
     */

    public synchronized void addNode(String nodeId){
        if(nodeId == null || nodeId.isEmpty()){
            throw new IllegalArgumentException(
                    "Nodeid cannot be null or empty. Got " + nodeId
            );
        }
        if(nodeToVnodes.containsKey(nodeId)) return ; // already present . Idempotent
        List<Long> positions = new ArrayList<>(virtualNodes);

        for(int i = 0; i < virtualNodes; i++){
            long position = MurmurHash3.hash64(nodeId+"#"+i);
            ring.put(position, nodeId);
            positions.add(position);
        }
        nodeToVnodes.put(nodeId,positions);
    }


    /**
     * Removes a physical node and all its virtual nodes from the ring.
     *
     * Keys previously owned by this node are automatically
     * reassigned to the next node clockwise from each removed
     * vnode position — no explicit reassignment needed.
     *
     * Because the removed node had 200 scattered vnodes, its keys
     * are redistributed across ALL remaining nodes rather than
     * dumping the entire load onto one neighbor. This is another
     * benefit of virtual nodes that is easy to miss.
     *
     * Idempotent: removing an unknown node is a no-op.
     *
     * @param nodeId the node to remove
     */

    public synchronized void removeNode(String nodeId){
        List<Long> positions = nodeToVnodes.remove(nodeId);
        if(positions == null){return;} // not present - idempotent

        for(Long position : positions){
            ring.remove(position);
        }
    }


    // ─────────────────────────────────────────────────────────────
    // Routing — the hot path
    // ─────────────────────────────────────────────────────────────

    /**
     * Returns the node that owns this key.
     *
     * Walks clockwise from hash(key) to the first vnode position.
     *
     * ceilingEntry(hash): smallest ring position >= hash.
     *   This IS "walk clockwise" — TreeMap does it in O(log n).
     *
     * Wrap-around: if the key hashes past the largest vnode position,
     *   ceilingEntry returns null. We wrap to firstEntry() — the
     *   smallest position — because the ring is a circle, not a line.
     *
     *   Example:
     *     ring positions: 1000, 3000, 5000
     *     key hashes to 6000
     *     ceilingEntry(6000) → null (nothing >= 6000)
     *     firstEntry()       → (1000, NodeA)   ← wrapped around ✓
     *
     * @param key the cache key
     * @return the nodeId that owns this key
     * @throws IllegalStateException if the ring is empty
     */

    public String getNode(String key){
        if(ring.isEmpty()){
            throw new IllegalStateException(
                    "Hash ring is empty — no nodes have been added. " +
                            "Call addNode() before routing keys."
            );
        }
        long hash = MurmurHash3.hash64(key);
        Map.Entry<Long,String> entry = ring.ceilingEntry(hash);
        // Past the last position → wrap around to the first
        if(entry == null){
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    /**
     * Returns the N distinct physical nodes that own this key,
     * walking clockwise from the key's position.
     *
     * This is the foundation for REPLICATION: store each key on
     * the primary node plus the next (count-1) distinct nodes
     * clockwise. If the primary fails, a replica already has the data.
     *
     * WHY "DISTINCT" MATTERS:
     *   Consecutive ring positions often belong to the SAME physical
     *   node (it has 200 vnodes scattered around). Naively taking
     *   the next 3 positions could return the same node three times —
     *   three copies on one machine is not replication.
     *   We skip duplicates and keep walking until we have `count`
     *   different physical nodes.
     *
     * Not used yet — included because replication is the natural
     * next extension and the walk logic belongs here, not in the router.
     *
     * @param key   the cache key
     * @param count how many distinct nodes to return
     * @return ordered list: primary first, then replicas.
     *         May be shorter than count if the cluster has fewer nodes.
     */
    public List<String> getNodes(String key, int count){
        if (ring.isEmpty()) {
            throw new IllegalStateException("Hash ring is empty");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }
        List<String> result = new ArrayList<>(count);
        long hash = MurmurHash3.hash64(key);

        // tailMap(hash, true) = all entries from hash to the end,
        // in ascending order — the clockwise walk from our position
        for (Map.Entry<Long, String> entry : ring.tailMap(hash, true).entrySet()) {
            if (!result.contains(entry.getValue())) {
                result.add(entry.getValue());
                if (result.size() == count) return result;
            }
        }

        // Reached the end of the ring — wrap around and continue
        // from the beginning to complete the circle
        for (Map.Entry<Long, String> entry : ring.entrySet()) {
            if (!result.contains(entry.getValue())) {
                result.add(entry.getValue());
                if (result.size() == count) return result;
            }
        }

        // Fewer distinct nodes exist than requested
        return result;
    }

    // ─────────────────────────────────────────────────────────────
    // Inspection
    // ─────────────────────────────────────────────────────────────

    /**
     * Returns all physical node ids currently in the ring.
     * Unmodifiable — callers cannot corrupt membership.
     */
    public synchronized Set<String> getNodes() {
        return Collections.unmodifiableSet(new java.util.HashSet<>(nodeToVnodes.keySet()));
    }

    /** Number of physical nodes. */
    public synchronized int nodeCount() {
        return nodeToVnodes.size();
    }

    /** Number of virtual node positions = nodeCount × virtualNodes. */
    public synchronized int ringSize() {
        return ring.size();
    }

    public int getVirtualNodes() {
        return virtualNodes;
    }

    public synchronized boolean containsNode(String nodeId) {
        return nodeToVnodes.containsKey(nodeId);
    }

}