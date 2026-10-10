package org.cache;

import org.cache.client.CacheClient;
import org.cache.cluster.ClusterConfig;
import org.cache.core.EvictionPolicy;
import org.cache.hash.ConsistentHashRing;

/**
 * EASY MODE — runs a 3-node cluster inside ONE JVM and proves routing works.
 *
 * Each CacheNode is a fully independent node with its own NIOServer on its
 * own port (7001, 7002, 7003). They talk to each other over real TCP on the
 * loopback interface — exactly like 3 separate machines, just on one host.
 *
 * What this demonstrates:
 *   1. Keys spread across all 3 nodes (printed from the ring).
 *   2. A client connects to ONE node (7001) and reads/writes EVERY key —
 *      keys owned by 7002/7003 are forwarded automatically. The cluster
 *      is transparent to the client.
 *
 * Run: right-click -> Run in IntelliJ. No arguments needed.
 */
public class LocalClusterDemo {

    public static void main(String[] args) throws Exception {
        String host  = "127.0.0.1";
        int[]  ports = { 7001, 7002, 7003 };

        // ── 1. Start all 3 nodes in this JVM ──────────────────────────
        CacheNode[] nodes = new CacheNode[ports.length];
        for (int i = 0; i < ports.length; i++) {

            // Every node knows the FULL member list; only localNode differs.
            ClusterConfig.Builder cluster =
                    new ClusterConfig.Builder().localNode(host + ":" + ports[i]);
            for (int p : ports) cluster.addNode(host + ":" + p);

            CacheConfig config = new CacheConfig.Builder()
                    .port(ports[i])
                    .capacity(100_000)
                    .evictionPolicy(EvictionPolicy.LRU)
                    .cluster(cluster.build())
                    .build();

            nodes[i] = new CacheNode(config);
            nodes[i].start();
        }

        Thread.sleep(300); // let all 3 selector threads come up

        // ── 2. Show which node OWNS each key ──────────────────────────
        // Every node builds an identical ring, so we can compute ownership
        // here with the same logic and it will match what the nodes decide.
        ConsistentHashRing ring = new ConsistentHashRing();
        for (int p : ports) ring.addNode(host + ":" + p);

        System.out.println("\n=== key ownership (decided by the hash ring) ===");
        for (int i = 1; i <= 10; i++) {
            String key = "user:" + i;
            System.out.println("  " + key + "  ->  " + ring.getNode(key));
        }

        // ── 3. Connect to ONE node and touch ALL keys ─────────────────
        // Keys owned by 7002 / 7003 get forwarded over TCP under the hood.
        System.out.println("\n=== round-trip through node " + ports[0] + " only ===");
        boolean allOk = true;
        try (CacheClient client = new CacheClient(host, ports[0])) {
            for (int i = 1; i <= 10; i++) {
                client.set("user:" + i, ("value-" + i).getBytes(), 0);
            }
            for (int i = 1; i <= 10; i++) {
                byte[] v   = client.get("user:" + i);
                String got = (v == null) ? "MISS" : new String(v);
                boolean ok = ("value-" + i).equals(got);
                allOk &= ok;
                System.out.println("  user:" + i + " = " + got + (ok ? "   OK" : "   FAIL"));
            }
        }

        System.out.println(allOk
                ? "\nAll keys round-tripped through a single node -> forwarding works."
                : "\nSomething failed -> check the logs above.");

        // ── 4. Shut everything down ───────────────────────────────────
        for (CacheNode n : nodes) n.stop();
    }
}
