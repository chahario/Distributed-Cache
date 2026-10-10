package org.cache.core;

/**
 * A lock-striped cache distributing keys across 32 independent partitions.
 *
 * WHY STRIPING?
 * ─────────────────────────────────────────────────────────────────
 * A single CachePartition has one lock. Under high concurrency,
 * all threads queue behind that one lock — throughput is limited
 * by how fast one thread can complete its operation.
 *
 * Striping creates 32 independent partitions, each with its own lock.
 * Two threads accessing keys in different partitions never contend.
 * Theoretical maximum: 32x throughput improvement vs single partition.
 * Practical improvement: significant, depends on key distribution.
 *
 * ROUTING:
 * ─────────────────────────────────────────────────────────────────
 * partition = (key.hashCode() & 0x7FFFFFFF) % 32
 *
 * Same key always routes to same partition — deterministic.
 * Keys distribute uniformly across partitions with good hash functions.
 *
 * CAPACITY:
 * ─────────────────────────────────────────────────────────────────
 * totalMaxEntries is divided equally across 32 partitions.
 * Each partition holds totalMaxEntries / 32 entries.
 * Uneven distribution (hot keys clustering in one partition) means
 * that partition evicts more aggressively than others.
 * With uniform hashing this is statistically rare.
 *
 * THREAD SAFETY:
 * ─────────────────────────────────────────────────────────────────
 * StripedCache itself has no lock — it just routes to partitions.
 * Each partition is fully thread-safe internally.
 * Concurrent calls to different partitions = full parallelism.
 * Concurrent calls to same partition = serialized by partition lock.
 */
public class StripedCache<K, V> {

    /**
     * Number of partitions.
     * Power of 2: enables bitwise AND optimization for routing.
     * 32: sweet spot between contention reduction and memory overhead.
     *
     * private static final: one value shared across all instances,
     * never changes, inlined by compiler at every usage site.
     */
    private static final int NUM_PARTITIONS = 32;

    /**
     * The 32 independent cache partitions.
     * Index 0 to 31. Each has its own map, eviction strategy, lock.
     * Array is final — the array reference never changes after construction.
     * The partition objects inside are mutable (they store cache data).
     */
    private final CachePartition<K, V>[] partitions;

    // ─────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────

    /**
     * Creates a StripedCache with LRU eviction.
     *
     * Convenience constructor — most callers want LRU.
     * Internally uses DefaultEntryFactory + LRUEvictionStrategy.
     *
     * @param totalMaxEntries total capacity across all 32 partitions
     */
    public StripedCache(int totalMaxEntries) {
        this(totalMaxEntries, EvictionPolicy.LRU);
    }

    /**
     * Creates a StripedCache with the specified eviction policy.
     *
     * Wires together the correct strategy + factory pair based on policy.
     * This is the only place in the codebase that knows which strategy
     * pairs with which factory — centralized wiring, no duplication.
     *
     * @param totalMaxEntries total capacity across all 32 partitions
     * @param policy          LRU or LFU
     */
    @SuppressWarnings("unchecked")
    public StripedCache(int totalMaxEntries, EvictionPolicy policy) {
        if (totalMaxEntries < NUM_PARTITIONS) {
            throw new IllegalArgumentException(
                    "totalMaxEntries must be >= " + NUM_PARTITIONS +
                            " (at least 1 per partition). Got: " + totalMaxEntries
            );
        }

        int perPartition = totalMaxEntries / NUM_PARTITIONS;

        // Array of generic type — unavoidable unchecked cast
        // Safe because we immediately fill every slot with a typed partition
        this.partitions = new CachePartition[NUM_PARTITIONS];

        for (int i = 0; i < NUM_PARTITIONS; i++) {
            partitions[i] = createPartition(perPartition, policy);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Core cache operations — all delegate to the correct partition
    // ─────────────────────────────────────────────────────────────

    /**
     * Returns value for key, or null on miss or expiry.
     *
     * Routes to the correct partition by key hash.
     * Partition handles locking, TTL check, eviction ordering.
     *
     * @return value if present and not expired, null otherwise
     */
    public V get(K key) {
        return partitionFor(key).get(key);
    }

    /**
     * Inserts or updates a key with no expiration.
     *
     * Convenience method — most cache entries don't need TTL.
     *
     * @param key   must be non-null
     * @param value may be null (negative caching)
     */
    public void put(K key, V value) {
        put(key, value, 0);
    }

    /**
     * Inserts or updates a key with optional TTL.
     *
     * Routes to correct partition.
     * Partition handles eviction if at capacity.
     *
     * @param key    must be non-null
     * @param value  may be null
     * @param ttlMs  0 or negative for no expiration
     */
    public void put(K key, V value, long ttlMs) {
        partitionFor(key).put(key, value, ttlMs);
    }

    /**
     * Removes a key from the cache.
     *
     * @return true if key existed and was removed, false if not found
     */
    public boolean delete(K key) {
        return partitionFor(key).delete(key);
    }

    /**
     * Returns true if the key exists and has not expired.
     *
     * Implemented as get() != null.
     * Side effect: updates eviction ordering (same as get()).
     * If you need a true read-only existence check, add a separate
     * containsKey() method that skips onAccess() — not implemented here.
     *
     * @return true if key is present and valid
     */
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    // ─────────────────────────────────────────────────────────────
    // Aggregate operations — touch all 32 partitions
    // ─────────────────────────────────────────────────────────────

    /**
     * Returns total number of entries across all partitions.
     *
     * Acquires each partition's readLock in sequence.
     * Not atomic — a concurrent put() or delete() may happen between
     * partition reads, making this a snapshot approximation.
     * Sufficient for monitoring and metrics. Not suitable for
     * capacity decisions (use per-partition size for those).
     *
     * Time: O(32) = O(1) effectively.
     */
    public int totalSize() {
        int total = 0;
        for (CachePartition<K, V> partition : partitions) {
            total += partition.size();
        }
        return total;
    }

    /**
     * Expires stale entries across all 32 partitions.
     * Called by TTLReaper on a background thread every N seconds.
     *
     * Each partition is scanned independently and sequentially.
     * Why sequentially and not in parallel?
     *   Each partition.expireStaleEntries() holds that partition's
     *   write lock for the duration of the scan.
     *   Parallel scanning would require 32 threads each holding a
     *   different partition's lock — more complex, and the reaper
     *   doesn't need to be fast, just thorough.
     *   Sequential is simpler and correct.
     *
     * @return total number of expired entries removed across all partitions
     */
    public int expireAllStale() {
        int total = 0;
        for (CachePartition<K, V> partition : partitions) {
            total += partition.expireStaleEntries();
        }
        return total;
    }

    /**
     * Removes all entries from all partitions.
     *
     * Not implemented as a cache operation — clearing a production
     * cache is dangerous (thundering herd on the backing store).
     * If needed for tests, access partitions directly.
     */

    // ─────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────

    /**
     * Routes a key to its partition.
     *
     * & 0x7FFFFFFF: clears the sign bit, ensuring positive value.
     * hashCode() can return negative numbers.
     * Negative % 32 = negative in Java — no partition at index -5.
     *
     * % NUM_PARTITIONS: maps to [0, 31].
     *
     * Same key always returns same partition — deterministic routing.
     * This is critical: a key stored via put() must be found via get()
     * in the same partition. If routing were non-deterministic,
     * get() would look in a different partition and always miss.
     *
     * @return the partition responsible for this key
     */
    private CachePartition<K, V> partitionFor(K key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        return partitions[(key.hashCode() & 0x7FFFFFFF) % NUM_PARTITIONS];
    }

    /**
     * Creates one partition wired with the correct strategy + factory.
     *
     * This is the only method that knows which strategy pairs with
     * which factory. Centralizing this pairing means:
     *   - Adding a new eviction policy = add one case here
     *   - CachePartition never needs to change
     *   - Misconfiguration (LFU strategy + LRU factory) is impossible
     *     when going through this method
     *
     * @param maxSize max entries for this partition
     * @param policy  eviction policy to use
     */
    private CachePartition<K, V> createPartition(int maxSize, EvictionPolicy policy) {
        return switch (policy) {
            case LRU -> new CachePartition<>(
                    maxSize,
                    new LRUEvictionStrategy<>(),
                    new DefaultEntryFactory<>()
            );
            case LFU -> new CachePartition<>(
                    maxSize,
                    new LFUEvictionStrategy<>(),
                    new LFUEntryFactory<>()
            );
        };
    }
}