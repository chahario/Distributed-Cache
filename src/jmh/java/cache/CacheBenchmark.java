package cache;

import org.cache.core.EvictionPolicy;
import org.cache.core.StripedCache;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH microbenchmark of the CORE in-process cache operations
 * (StripedCache.get / put) — no network involved.
 *
 * This isolates the data-structure cost: hashing to a partition, taking the
 * partition lock, the ConcurrentHashMap lookup, and the LRU reordering.
 * Because there is no TCP round trip, these numbers are far lower than the
 * end-to-end networked p99 measured by LatencyBenchmark — this is the raw
 * ceiling of the cache itself.
 *
 * Modes:
 *   Throughput  → operations per second
 *   SampleTime  → latency distribution, including p99 / p99.9
 *
 * Run (after the JMH annotation processor has generated the harness):
 *   java -cp <cp> org.openjdk.jmh.Main "cache.CacheBenchmark" -f 1
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class CacheBenchmark {

    static final int KEYSPACE = 100_000;

    private StripedCache<String, byte[]> cache;
    private String[] keys;
    private byte[] value;

    /** Per-thread RNG so key selection doesn't become a shared-state bottleneck. */
    @State(Scope.Thread)
    public static class Rnd {
        final Random r = new Random();
    }

    @Setup(Level.Trial)
    public void setup() {
        cache = new StripedCache<>(1_000_000, EvictionPolicy.LRU);
        value = new byte[64];

        // Pre-build keys (so we don't measure string concatenation) and
        // pre-populate so GETs hit an existing entry.
        keys = new String[KEYSPACE];
        for (int i = 0; i < KEYSPACE; i++) {
            keys[i] = "k:" + i;
            cache.put(keys[i], value, 0);
        }
    }

    @Benchmark
    @Threads(8)
    public byte[] get(Rnd rnd) {
        // returned value is consumed by JMH → no dead-code elimination
        return cache.get(keys[rnd.r.nextInt(KEYSPACE)]);
    }

    @Benchmark
    @Threads(8)
    public void put(Rnd rnd) {
        // side effect on the cache → not eliminated
        cache.put(keys[rnd.r.nextInt(KEYSPACE)], value, 0);
    }
}
