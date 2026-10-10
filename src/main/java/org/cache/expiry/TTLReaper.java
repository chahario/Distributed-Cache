package org.cache.expiry;

import org.cache.core.StripedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background thread that periodically scans all cache partitions
 * and removes entries whose TTL has expired.
 *
 * WHY THIS EXISTS:
 * ─────────────────────────────────────────────────────────────────
 * Lazy expiration in CachePartition.get() only fires when a key
 * is READ. A key inserted with TTL but never read again stays in
 * memory forever without this reaper.
 *
 * Example:
 *   put("session:abc", data, 30_000)  → 30 second TTL
 *   user closes browser               → key never read again
 *   WITHOUT reaper: lives in memory until evicted by capacity
 *   WITH reaper:    removed within reapIntervalMs of expiry
 *
 * TWO MECHANISMS TOGETHER:
 *   Lazy expiration → never return stale data on read
 *   TTLReaper       → reclaim memory even for unread expired keys
 *
 * CRITICAL — SILENT FAILURE MODE:
 * ─────────────────────────────────────────────────────────────────
 * ScheduledExecutorService SILENTLY cancels the schedule if the
 * task throws an uncaught exception.
 *
 * Without try-catch inside reap():
 *   Cycle 1: runs fine
 *   Cycle 2: throws RuntimeException → schedule CANCELLED silently
 *   Cycle 3+: NEVER RUNS
 *   Expired entries accumulate forever → memory leak
 *   No error anywhere. You have no idea why memory grows.
 *
 * Solution: catch ALL exceptions inside reap(), log, continue.
 * The reaper must NEVER stop running due to an exception.
 *
 * THREADING:
 * ─────────────────────────────────────────────────────────────────
 * Single daemon thread.
 *
 * Why single thread?
 *   Sequential partition scanning is sufficient.
 *   32 partitions × ~100µs = ~3.2ms total scan.
 *   At 1s intervals = 0.32% CPU usage.
 *   Adding more threads adds complexity with no real benefit.
 *
 * Why daemon thread?
 *   JVM exits when all non-daemon threads finish.
 *   Non-daemon reaper would prevent JVM shutdown after main() returns.
 *   Daemon thread is killed automatically on JVM exit.
 */
public class TTLReaper {

    private static final Logger log = LoggerFactory.getLogger(TTLReaper.class);

    /**
     * Default scan interval: 1 second.
     * Expired entries live at most 1 second past their TTL.
     * Lower = faster cleanup, higher CPU.
     * Higher = slower cleanup, lower CPU.
     */
    private static final long DEFAULT_REAP_INTERVAL_MS = 1000;

    private final StripedCache<?, ?> cache;
    private final long reapIntervalMs;
    private final ScheduledExecutorService scheduler;

    /**
     * Total expired entries removed across all reap cycles.
     *
     * Why AtomicLong not plain long?
     *   Reaper thread writes this.
     *   Monitoring thread might read this.
     *   Plain long: non-atomic on 32-bit JVMs — monitoring thread
     *   could see half-updated value (two 32-bit reads of one 64-bit write).
     *   AtomicLong: single atomic read/write on all JVMs. Safe.
     */
    private final AtomicLong totalExpired = new AtomicLong(0);

    /**
     * How many reap cycles have completed.
     * AtomicLong for same reason as totalExpired.
     */
    private final AtomicLong reapCycles = new AtomicLong(0);

    // ─────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────

    /**
     * Creates reaper with default 1 second interval.
     */
    public TTLReaper(StripedCache<?, ?> cache) {
        this(cache, DEFAULT_REAP_INTERVAL_MS);
    }

    /**
     * Creates reaper with custom interval.
     *
     * @param cache          the cache to reap expired entries from
     * @param reapIntervalMs how often to scan in milliseconds
     */
    public TTLReaper(StripedCache<?, ?> cache, long reapIntervalMs) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache cannot be null");
        }
        if (reapIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "reapIntervalMs must be positive. Got: " + reapIntervalMs
            );
        }

        this.cache = cache;
        this.reapIntervalMs = reapIntervalMs;

        /**
         * Single scheduled thread with custom ThreadFactory.
         *
         * ThreadFactory lambda: r -> { ... }
         *   r = the Runnable the executor wants to run
         *   we wrap it in a named daemon Thread
         *
         * Why custom ThreadFactory?
         *   1. Name the thread "ttl-reaper"
         *      Default name is "pool-1-thread-1" — useless in thread dumps
         *      Named thread: instantly visible in logs and debugger
         *
         *   2. setDaemon(true)
         *      JVM exits without waiting for daemon threads
         *      Without this: reaper prevents JVM shutdown
         */
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ttl-reaper");
            t.setDaemon(true);
            return t;
        });
    }

    // ─────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────

    /**
     * Starts the background reaper.
     *
     * scheduleAtFixedRate vs scheduleWithFixedDelay:
     *
     *   scheduleAtFixedRate(task, delay, period):
     *     Runs at fixed CLOCK intervals.
     *     t=1s: run, t=2s: run, t=3s: run
     *     If task takes longer than period → next run queued immediately
     *     after current finishes (no overlap with single thread).
     *
     *   scheduleWithFixedDelay(task, delay, period):
     *     Waits fixed time AFTER task finishes.
     *     t=1s: starts, finishes at t=1.3s → t=2.3s: next run
     *     Drift accumulates over time.
     *
     *   We use AtFixedRate:
     *     "entries expire within ~1s of TTL" — consistent guarantee
     *     WithFixedDelay gives "~1s + accumulated drift" — weaker
     */
    public void start() {
        log.info("TTLReaper starting. interval={}ms", reapIntervalMs);

        scheduler.scheduleAtFixedRate(
                this::reap,          // method reference — runs every interval
                reapIntervalMs,      // wait this long before FIRST run
                reapIntervalMs,      // wait this long between SUBSEQUENT runs
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stops the reaper gracefully.
     *
     * Shutdown sequence:
     *   1. shutdown()            → stop accepting new tasks
     *                              let current task finish
     *   2. awaitTermination(5s)  → wait up to 5s for current scan to finish
     *   3. shutdownNow()         → force-interrupt if still running after 5s
     *
     * Why not shutdownNow() immediately?
     *   Reaper mid-scan would be interrupted.
     *   Partition write lock IS released (finally block always runs).
     *   But waiting lets current scan complete cleanly — no partial state.
     *
     * Thread.currentThread().interrupt():
     *   If stop() caller is interrupted during awaitTermination(),
     *   we restore the interrupt signal so caller can detect it.
     */
    public void stop() {
        log.info("TTLReaper stopping...");

        scheduler.shutdown();

        try {
            boolean finished = scheduler.awaitTermination(5, TimeUnit.SECONDS);
            if (!finished) {
                log.warn("TTLReaper did not finish within 5s — forcing shutdown");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("TTLReaper stop() interrupted — forcing shutdown");
            scheduler.shutdownNow();
            Thread.currentThread().interrupt(); // restore interrupt signal
        }

        log.info("TTLReaper stopped. cycles={} totalExpired={}",
                reapCycles.get(), totalExpired.get());
    }

    // ─────────────────────────────────────────────────────────────
    // The actual reap task
    // ─────────────────────────────────────────────────────────────

    /**
     * One reap cycle — called every reapIntervalMs.
     *
     * MUST catch all exceptions — see class Javadoc for why.
     *
     * We catch Exception not Throwable:
     *   Exception: recoverable — log and continue
     *   Error (OutOfMemoryError, StackOverflowError): JVM-level problem
     *              we cannot recover → let it propagate → JVM handles shutdown
     *
     * Logging strategy:
     *   Only log when something expired OR every 60 cycles (1 min with 1s interval)
     *   Avoids flooding logs with "expired 0 entries" every second
     */
    private void reap() {
        try {
            long start   = System.currentTimeMillis();
            int  expired = cache.expireAllStale();
            long duration = System.currentTimeMillis() - start;

            long cycle = reapCycles.incrementAndGet();
            totalExpired.addAndGet(expired);

            if (expired > 0 || cycle % 60 == 0) {
                log.debug(
                        "TTLReaper cycle={} expired={} duration={}ms totalExpired={}",
                        cycle, expired, duration, totalExpired.get()
                );
            }

        } catch (Exception e) {
            // Log and swallow — reaper MUST keep running
            // DO NOT rethrow — kills the schedule silently
            log.error(
                    "TTLReaper error during reap cycle. " +
                            "Reaper will continue on next interval.", e
            );
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Monitoring
    // ─────────────────────────────────────────────────────────────

    public long getTotalExpired() { return totalExpired.get(); }
    public long getReapCycles()   { return reapCycles.get();   }
    public boolean isRunning()    { return !scheduler.isShutdown(); }
}