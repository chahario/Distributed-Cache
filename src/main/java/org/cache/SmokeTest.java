package org.cache;

import org.cache.client.CacheClient;
import org.cache.core.EvictionPolicy;

/**
 * Manual smoke test — run this in the debugger.
 *
 * Verifies the full stack works end to end before we benchmark:
 *   CacheNode → StripedCache → CachePartition → EvictionStrategy
 *   NIOServer → ConnectionHandler → BinaryCodec → CacheClient
 *
 * Run with the debugger and step through. Breakpoint suggestions
 * are marked with  ★ BREAKPOINT  in the comments below.
 */
public class SmokeTest {

    public static void main(String[] args) throws Exception {

        System.out.println("═══════════════════════════════════════");
        System.out.println("  PHASE 1 — Embedded (no network)");
        System.out.println("═══════════════════════════════════════");

        CacheNode node = new CacheNode(
                new CacheConfig.Builder()
                        .port(7001)
                        .capacity(320)              // 10 per partition — easy to fill
                        .evictionPolicy(EvictionPolicy.LRU)
                        .reaperIntervalMs(500)      // fast reaper for TTL testing
                        .build()
        );

        node.start();

        // ── Test 1: basic put/get ────────────────────────────────
        // ★ BREAKPOINT: CachePartition.put()  — watch the write lock,
        //   the factory call, and evictionStrategy.onInsert()
        node.put("user:1", "Dinesh".getBytes());

        // ★ BREAKPOINT: CachePartition.get()  — watch isExpired(),
        //   then LRUEvictionStrategy.onAccess() → moveToHead()
        byte[] v1 = node.get("user:1");
        check("put/get", "Dinesh".equals(new String(v1)));

        // ── Test 2: cache miss ───────────────────────────────────
        byte[] missing = node.get("does-not-exist");
        check("miss returns null", missing == null);

        // ── Test 3: update in place ──────────────────────────────
        // ★ BREAKPOINT: CachePartition.put() — this should take the
        //   UPDATE path (existing != null), calling setValue +
        //   onAccess, NOT creating a new entry
        node.put("user:1", "Updated".getBytes());
        byte[] v2 = node.get("user:1");
        check("update in place", "Updated".equals(new String(v2)));
        check("update did not grow size", node.size() == 1);

        // ── Test 4: delete ───────────────────────────────────────
        boolean deleted = node.delete("user:1");
        check("delete returns true", deleted);
        check("deleted key is gone", node.get("user:1") == null);
        check("delete of absent key", !node.delete("user:1"));

        // ── Test 5: LRU eviction ─────────────────────────────────
        // Fill one partition past its 10-entry limit.
        // Keys are chosen so many land in the same partition —
        // we cannot control which, so we insert enough that at
        // least one partition definitely overflows.
        System.out.println("\n--- filling cache to force eviction ---");
        for (int i = 0; i < 500; i++) {
            node.put("fill:" + i, ("v" + i).getBytes());
        }

        // ★ BREAKPOINT: LRUEvictionStrategy.evict()
        //   Watch dummyTail.prev, then unlinkNode, then size--
        int size = node.size();
        System.out.println("size after 500 inserts into 320-capacity cache: " + size);
        check("eviction kept size bounded", size <= 320);

        // ── Test 6: TTL lazy expiry ──────────────────────────────
        node.put("temp:1", "expires".getBytes(), 300);   // 300ms TTL
        check("TTL key readable immediately", node.get("temp:1") != null);

        Thread.sleep(400);

        // ★ BREAKPOINT: CachePartition.get() — the isExpired() branch
        //   Watch map.remove + evictionStrategy.onRemove
        check("TTL key expired on read", node.get("temp:1") == null);

        // ── Test 7: TTL reaper (background) ──────────────────────
        // Insert a key with TTL and NEVER read it.
        // Only the reaper can remove it.
        node.put("reaped:1", "bye".getBytes(), 200);
        int sizeBefore = node.size();

        System.out.println("\n--- waiting for TTL reaper ---");
        Thread.sleep(1500);   // reaper runs every 500ms

        // ★ BREAKPOINT: TTLReaper.reap()  or
        //   CachePartition.expireStaleEntries()
        int sizeAfter = node.size();
        System.out.println("size before reaper: " + sizeBefore
                + ", after: " + sizeAfter);
        check("reaper removed unread expired key", sizeAfter < sizeBefore);

        node.stop();

        System.out.println("\n═══════════════════════════════════════");
        System.out.println("  PHASE 2 — Over TCP");
        System.out.println("═══════════════════════════════════════");

        CacheNode server = new CacheNode(
                new CacheConfig.Builder()
                        .port(7001)
                        .capacity(1000)
                        .build()
        );
        server.start();

        Thread.sleep(200);   // let the selector thread come up

        try (CacheClient client = new CacheClient("localhost", 7001)) {

            // ── Test 8: SET over the wire ────────────────────────
            // ★ BREAKPOINT: BinaryCodec.encodeRequest()
            //   Inspect the 12-byte header being built
            // ★ BREAKPOINT: ConnectionHandler.feed()
            //   Watch READING_HEADER → parse magic → READING_BODY
            // ★ BREAKPOINT: NIOServer.processRequest()
            //   Confirm it reaches the SET case
            client.set("net:1", "over-tcp".getBytes(), 0);

            // ── Test 9: GET over the wire ────────────────────────
            // ★ BREAKPOINT: NIOServer.handleWrite()
            //   Watch response.hasRemaining() and the OP_READ switch
            byte[] netVal = client.get("net:1");
            check("TCP set/get", "over-tcp".equals(new String(netVal)));

            // ── Test 10: MISS over the wire ──────────────────────
            check("TCP miss returns null", client.get("net:nope") == null);

            // ── Test 11: DELETE over the wire ────────────────────
            check("TCP delete", client.delete("net:1"));
            check("TCP delete removed it", client.get("net:1") == null);
            check("TCP delete absent key", !client.delete("net:1"));

            // ── Test 12: TTL over the wire ───────────────────────
            // Protocol carries TTL in SECONDS, server converts to ms
            client.set("net:ttl", "short".getBytes(), 1);   // 1 second
            check("TTL key present", client.get("net:ttl") != null);
            Thread.sleep(1300);
            check("TTL key expired over TCP", client.get("net:ttl") == null);

            // ── Test 13: many requests, one connection ───────────
            // Verifies handler.reset() works — the connection must
            // serve request after request without corruption
            System.out.println("\n--- 1000 requests on one connection ---");
            boolean allOk = true;
            for (int i = 0; i < 1000; i++) {
                client.set("loop:" + i, ("val" + i).getBytes(), 0);
                byte[] back = client.get("loop:" + i);
                if (back == null || !("val" + i).equals(new String(back))) {
                    System.out.println("  FAILED at i=" + i);
                    allOk = false;
                    break;
                }
            }
            check("1000 sequential requests on one connection", allOk);

            // ── Test 14: large value (multi-read path) ───────────
            // 8KB value exceeds the 4KB read buffer, so the server
            // must handle it across multiple READ events via the
            // ConnectionHandler state machine
            byte[] big = new byte[8192];
            for (int i = 0; i < big.length; i++) big[i] = (byte)(i % 256);

            // ★ BREAKPOINT: ConnectionHandler.feed()
            //   This should return false on the first call
            //   (body incomplete) and true on a later one
            client.set("net:big", big, 0);
            byte[] bigBack = client.get("net:big");
            check("8KB value round trip",
                    bigBack != null && bigBack.length == 8192 && bigBack[100] == big[100]);
        }

        server.stop();

        System.out.println("\n═══════════════════════════════════════");
        System.out.println("  PHASE 3 — Concurrency");
        System.out.println("═══════════════════════════════════════");

        CacheNode concurrent = new CacheNode(
                new CacheConfig.Builder().port(7002).capacity(10_000).build()
        );
        concurrent.start();

        // 50 threads, 1000 ops each, embedded API (no network)
        // If this deadlocks or corrupts, the locking is wrong
        int threads = 50;
        int opsPerThread = 1000;
        Thread[] workers = new Thread[threads];
        final boolean[] failed = { false };

        long start = System.currentTimeMillis();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            workers[t] = new Thread(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "t" + threadId + ":k" + (i % 100);
                        concurrent.put(key, ("v" + i).getBytes());
                        concurrent.get(key);
                        if (i % 10 == 0) concurrent.delete(key);
                    }
                } catch (Exception e) {
                    System.out.println("Thread " + threadId + " FAILED: " + e);
                    e.printStackTrace();
                    failed[0] = true;
                }
            }, "worker-" + t);
            workers[t].start();
        }

        for (Thread w : workers) w.join(30_000);

        long elapsed = System.currentTimeMillis() - start;
        int totalOps = threads * opsPerThread * 2;   // put + get

        check("50 threads × 1000 ops completed", !failed[0]);
        System.out.println("  " + totalOps + " ops in " + elapsed + "ms"
                + "  (~" + (totalOps * 1000L / Math.max(elapsed,1)) + " ops/sec)");
        System.out.println("  final size: " + concurrent.size());

        concurrent.stop();

        System.out.println("\n═══════════════════════════════════════");
        System.out.println("  ALL PHASES COMPLETE");
        System.out.println("═══════════════════════════════════════");
    }

    private static void check(String label, boolean condition) {
        System.out.println((condition ? "  PASS  " : "  FAIL  ") + label);
        if (!condition) {
            throw new AssertionError("FAILED: " + label);
        }
    }
}