package org.cache;

import org.cache.cluster.ClusterConfig;
import org.cache.core.EvictionPolicy;

/**
 * REALISTIC MODE — starts ONE cluster node as its own process.
 *
 * Run this THREE times in three separate terminals, once per port, to
 * simulate three physical machines:
 *
 *   java -cp <classpath> org.cache.ClusterMain 7001
 *   java -cp <classpath> org.cache.ClusterMain 7002
 *   java -cp <classpath> org.cache.ClusterMain 7003
 *
 * (Or make 3 run configurations in IntelliJ with program arg 7001/7002/7003.)
 *
 * Every node is told the SAME member list, so all three build an identical
 * hash ring and agree on who owns which key. A client may connect to any one
 * of them; requests for keys owned elsewhere are forwarded automatically.
 */
public class ClusterMain {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println(
                    "Usage: ClusterMain <myPort>\n" +
                    "  members default to 127.0.0.1:{7001,7002,7003}");
            return;
        }

        String host    = "127.0.0.1";
        int    myPort  = Integer.parseInt(args[0]);
        int[]  members = { 7001, 7002, 7003 };

        // Same member list on every node; only localNode changes per process.
        ClusterConfig.Builder cluster =
                new ClusterConfig.Builder().localNode(host + ":" + myPort);
        for (int p : members) cluster.addNode(host + ":" + p);

        CacheConfig config = new CacheConfig.Builder()
                .port(myPort)
                .capacity(100_000)
                .evictionPolicy(EvictionPolicy.LRU)
                .cluster(cluster.build())
                .build();

        CacheNode node = new CacheNode(config);
        node.start();

        System.out.println("Cluster node " + host + ":" + myPort +
                " started. Press Ctrl+C to stop.");

        // Keep the process alive; the shutdown hook stops the node on Ctrl+C.
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            node.stop();
        }
    }
}
