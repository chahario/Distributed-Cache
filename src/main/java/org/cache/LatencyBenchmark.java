package org.cache;

import org.cache.client.CacheClient;
import org.cache.core.EvictionPolicy;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Real latency + throughput benchmark for the cache, measured OVER TCP
 * (the NIO server + binary protocol), under concurrent load.
 *
 * Each client connection is its own thread with its own socket (CacheClient
 * is single-threaded by design). Every operation's round-trip latency is
 * timed with System.nanoTime(); percentiles are computed from the full,
 * sorted sample set — no sampling, no external library.
 *
 * Usage:  LatencyBenchmark <connections> <opsPerConn> <keyspace> <writePct>
 * Default: 100 connections x 5000 ops, 50k keys, 10% writes.
 */
public class LatencyBenchmark {

    public static void main(String[] args) throws Exception {
        int    port        = 7099;
        int    connections = arg(args, 0, 100);
        int    opsPerConn  = arg(args, 1, 5000);
        int    keyspace    = arg(args, 2, 50_000);
        double writeRatio  = arg(args, 3, 10) / 100.0;

        CacheNode server = new CacheNode(new CacheConfig.Builder()
                .port(port).capacity(1_000_000)
                .evictionPolicy(EvictionPolicy.LRU)
                .reaperIntervalMs(1000)
                .build());
        server.start();
        Thread.sleep(300);

        // Pre-populate so reads hit instead of missing
        byte[] val = new byte[64];
        Arrays.fill(val, (byte) 'x');
        try (CacheClient warm = new CacheClient("127.0.0.1", port)) {
            for (int i = 0; i < keyspace; i++) warm.set("k:" + i, val, 0);
        }

        System.out.printf(">>> load: %d concurrent connections x %d ops = %,d total ops, %.0f%% reads%n",
                connections, opsPerConn, (long) connections * opsPerConn, (1 - writeRatio) * 100);

        // Warmup (lets the JIT compile the hot paths before we measure)
        runPhase("warmup",  port, Math.min(connections, 50), 1000, keyspace, writeRatio, false);
        // Measurement
        runPhase("measure", port, connections, opsPerConn, keyspace, writeRatio, true);

        server.stop();
    }

    private static void runPhase(String name, int port, int connections, int opsPerConn,
                                 int keyspace, double writeRatio, boolean report) throws Exception {
        long[][] latencies = new long[connections][];
        Thread[] workers   = new Thread[connections];
        CountDownLatch ready = new CountDownLatch(connections);
        CountDownLatch go    = new CountDownLatch(1);

        for (int t = 0; t < connections; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
                long[] mine = new long[opsPerConn];
                byte[] val  = new byte[64];
                Arrays.fill(val, (byte) 'y');
                Random rnd  = new Random(tid);
                try (CacheClient c = new CacheClient("127.0.0.1", port)) {
                    ready.countDown();
                    go.await();                       // all threads start together
                    for (int i = 0; i < opsPerConn; i++) {
                        String key = "k:" + rnd.nextInt(keyspace);
                        long s = System.nanoTime();
                        if (rnd.nextDouble() < writeRatio) c.set(key, val, 0);
                        else                              c.get(key);
                        mine[i] = System.nanoTime() - s;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                latencies[tid] = mine;
            }, "load-" + t);
            workers[t].start();
        }

        ready.await();
        long start = System.nanoTime();
        go.countDown();
        for (Thread w : workers) w.join();
        long elapsedNs = System.nanoTime() - start;

        if (!report) { System.out.println("    (" + name + " done)"); return; }

        int total = connections * opsPerConn;
        long[] all = new long[total];
        int idx = 0;
        for (long[] arr : latencies) { System.arraycopy(arr, 0, all, idx, arr.length); idx += arr.length; }
        Arrays.sort(all);

        double ms = 1e-6;
        System.out.println("========== RESULTS (round-trip over TCP + NIO) ==========");
        System.out.printf("total ops      : %,d%n", total);
        System.out.printf("throughput     : %,.0f ops/sec%n", total / (elapsedNs * 1e-9));
        System.out.printf("p50 latency    : %.3f ms%n", all[(int) (total * 0.50)] * ms);
        System.out.printf("p90 latency    : %.3f ms%n", all[(int) (total * 0.90)] * ms);
        System.out.printf("p99 latency    : %.3f ms%n", all[(int) (total * 0.99)] * ms);
        System.out.printf("p99.9 latency  : %.3f ms%n", all[(int) (total * 0.999)] * ms);
        System.out.printf("max latency    : %.3f ms%n", all[total - 1] * ms);
        System.out.println("=========================================================");
    }

    private static int arg(String[] a, int i, int def) {
        return a.length > i ? Integer.parseInt(a[i]) : def;
    }
}
