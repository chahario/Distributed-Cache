package org.cache.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A single independent partition of the distributed cache.
 *
 * CachePartition is the unit where all four core components meet:
 *   1. Primary index     → ConcurrentHashMap<K, CacheEntry<K,V>>
 *   2. Eviction ordering → EvictionStrategy<K,V>
 *   3. Entry creation    → CacheEntryFactory<K,V>
 *   4. Concurrency       → ReentrantReadWriteLock
 *
 * StripedCache creates 32 of these. Keys are hashed to partitions
 * so operations on different partitions never contend.
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY ConcurrentHashMap AS THE PRIMARY INDEX?
 * ─────────────────────────────────────────────────────────────────
 * We already have a ReentrantReadWriteLock protecting all operations.
 * So why ConcurrentHashMap and not plain HashMap?
 *
 * HashMap is not safe even with external locking in one edge case:
 * expireStaleEntries() iterates the map while potentially removing
 * entries. HashMap's iterator throws ConcurrentModificationException
 * if the map is structurally modified during iteration — even from
 * the same thread if done incorrectly.
 *
 * ConcurrentHashMap's iterator is weakly consistent — it tolerates
 * removals during iteration without throwing. This makes
 * expireStaleEntries() safe: collect expired keys while iterating,
 * then remove them.
 *
 * We still hold the write lock — CHM's own internal locking is
 * redundant but harmless. The external write lock ensures atomicity
 * of compound operations (map + strategy together).
 *
 * ─────────────────────────────────────────────────────────────────
 * LOCK STRATEGY
 * ─────────────────────────────────────────────────────────────────
 * ALL operations use writeLock — including get().
 *
 * Why get() uses writeLock:
 *   LRU: get() calls onAccess() which calls moveToHead() —
 *        8 non-atomic pointer mutations on the DLL.
 *   LFU: get() calls onAccess() which mutates frequency,
 *        lastAccessedAt, freqBuckets, keyToBucket — 4 structures.
 *   Neither is safe for concurrent access. writeLock required.
 *
 * Optimization path (not implemented here):
 *   Approximate LRU: skip moveToHead() on get(), use readLock.
 *   Trade eviction accuracy for read throughput.
 *   Redis uses this approach — sample 5 random keys, evict oldest.
 *
 * ─────────────────────────────────────────────────────────────────
 * THREAD SAFETY CONTRACT
 * ─────────────────────────────────────────────────────────────────
 * All public methods acquire the write lock internally.
 * Callers (StripedCache, TTLReaper) do NOT need to acquire any lock.
 * This class is fully thread-safe from the outside.
 */
public class CachePartition<K, V> {

    /**
     * Primary key-value index.
     *
     * ConcurrentHashMap chosen over HashMap for safe iteration
     * during expireStaleEntries() — see class Javadoc above.
     *
     * Initial capacity = maxSize to avoid rehashing.
     * Rehashing copies the entire table — expensive and causes
     * a latency spike. Pre-sizing eliminates this.
     *
     * Load factor 0.75 (default): table rehashes when 75% full.
     * Since we cap at maxSize and evict before reaching it,
     * we never actually rehash after construction.
     */
    private final ConcurrentHashMap<K, CacheEntry<K, V>> map;

    /**
     * Eviction strategy — LRU or LFU, injected at construction.
     *
     * CachePartition never calls LRU-specific or LFU-specific methods.
     * It only calls the four interface methods: onInsert, onAccess,
     * onRemove, evict(). This is the Strategy pattern in action —
     * swapping eviction algorithm = swapping one constructor argument.
     */
    private final EvictionStrategy<K, V> evictionStrategy;

    /**
     * Creates the correct CacheEntry subtype.
     * DefaultEntryFactory → CacheEntry (for LRU)
     * LFUEntryFactory     → LFUCacheEntry (for LFU)
     *
     * CachePartition never calls new CacheEntry() directly.
     * Always goes through the factory. This keeps CachePartition
     * decoupled from the concrete entry type.
     */
    private final CacheEntryFactory<K, V> entryFactory;

    /**
     * Maximum number of entries this partition holds.
     * When size reaches maxSize, eviction runs before each insert.
     *
     * Total cache capacity = maxSize × 32 (number of partitions).
     * StripedCache divides totalMaxEntries by 32 and passes the
     * result as maxSize to each partition.
     */
    private final int maxSize;

    /**
     * The concurrency control.
     *
     * One lock per partition — this is the "striping" in StripedCache.
     * 32 partitions = 32 independent locks = up to 32x parallelism
     * vs a single global lock.
     *
     * Two keys hashing to different partitions never contend.
     * Two keys hashing to the same partition share one lock.
     * With 32 partitions and uniform hashing, contention probability
     * for any two random keys = 1/32 = 3.1%.
     */
    private final ReentrantReadWriteLock rwLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private final ReentrantReadWriteLock.ReadLock  readLock;

    // ─────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────

    /**
     * Constructs a partition with the given capacity, strategy, and factory.
     *
     * The strategy and factory must be compatible:
     *   LRUEvictionStrategy + DefaultEntryFactory  ✓
     *   LFUEvictionStrategy + LFUEntryFactory      ✓
     *   LFUEvictionStrategy + DefaultEntryFactory  ✗ (castToLFU throws)
     *
     * Incompatible combinations are caught at runtime on first insert.
     * A future improvement: a typed CacheConfig object that enforces
     * compatible combinations at compile time.
     *
     * @param maxSize          max entries before eviction kicks in
     * @param evictionStrategy LRU or LFU strategy instance
     * @param entryFactory     creates correct CacheEntry subtype
     */
    public CachePartition(
            int maxSize,
            EvictionStrategy<K, V> evictionStrategy,
            CacheEntryFactory<K, V> entryFactory) {

        if (maxSize <= 0) {
            throw new IllegalArgumentException(
                    "maxSize must be positive, got: " + maxSize);
        }

        this.maxSize          = maxSize;
        this.evictionStrategy = evictionStrategy;
        this.entryFactory     = entryFactory;
        this.map              = new ConcurrentHashMap<>(maxSize);
        this.rwLock           = new ReentrantReadWriteLock();
        this.writeLock        = rwLock.writeLock();
        this.readLock         = rwLock.readLock();
    }

    // ─────────────────────────────────────────────────────────────
    // Public operations
    // ─────────────────────────────────────────────────────────────

    /**
     * Returns the value for key, or null on miss or expiry.
     *
     * Two null-return cases:
     *   MISS:    key was never inserted, or was evicted/deleted.
     *   EXPIRED: key exists but has outlived its TTL.
     *            Entry is removed lazily here (lazy expiration).
     *
     * Why writeLock and not readLock?
     *   onAccess() mutates eviction ordering (DLL for LRU, buckets for LFU).
     *   These mutations are not safe for concurrent access.
     *   See class-level Javadoc for full reasoning.
     *
     * Lazy expiration:
     *   We check isExpired() on every get(). If expired, we remove the
     *   entry here rather than waiting for the TTL reaper.
     *   Pro: expired entries don't return stale values even if reaper
     *        hasn't run yet.
     *   Con: a key that is never read again lives in memory until
     *        the reaper scans for it. Both mechanisms together give
     *        correctness (lazy) + memory hygiene (reaper).
     *
     * @return value if key exists and is not expired, null otherwise
     */
    public V get(K key) {
        writeLock.lock();
        try {
            CacheEntry<K, V> entry = map.get(key);

            // Case 1: key not in map at all
            if (entry == null) return null;

            // Case 2: key in map but TTL has expired
            if (entry.isExpired()) {
                // Remove from both map and eviction strategy
                map.remove(key);
                evictionStrategy.onRemove(entry);
                return null;  // treat as miss
            }

            // Case 3: valid entry — update eviction ordering
            evictionStrategy.onAccess(entry);
            return entry.getValue();

        } finally {
            writeLock.unlock();  // ALWAYS releases, even if exception thrown
        }
    }

    /**
     * Inserts or updates a key-value pair with optional TTL.
     *
     * Two distinct paths:
     *
     * UPDATE (key already exists):
     *   Update value in place — avoids allocation + DLL re-link.
     *   Notify strategy via onAccess() — entry is now MRU (LRU)
     *   or gets frequency increment (LFU).
     *   TTL cannot be updated (ttlMs is final in CacheEntry).
     *   If new TTL is needed, delete + re-insert.
     *
     * INSERT (key is new):
     *   If at capacity → evict LRU/LFU entry first.
     *   Create new entry via factory.
     *   Put in map.
     *   Notify strategy via onInsert().
     *
     * Why evict BEFORE inserting?
     *   If we insert first, the map has maxSize+1 entries momentarily.
     *   Then eviction removes one, back to maxSize.
     *   This brief overshoot is harmless but wastes one slot.
     *   Evicting first keeps the map always at or below maxSize.
     *
     * @param key    must be non-null
     * @param value  may be null (negative caching)
     * @param ttlMs  0 or negative for no expiration
     */
    public void put(K key, V value, long ttlMs) {
        writeLock.lock();
        try {
            CacheEntry<K, V> existing = map.get(key);

            if (existing != null) {
                // UPDATE PATH
                // Check if existing entry is expired
                if (existing.isExpired()) {
                    // Treat as new insert — remove stale entry first
                    map.remove(key);
                    evictionStrategy.onRemove(existing);
                    // Fall through to INSERT PATH below
                } else {
                    // Valid existing entry — update value in place
                    existing.setValue(value);
                    evictionStrategy.onAccess(existing);
                    return;  // done
                }
            }

            // INSERT PATH
            // Evict if at capacity
            if (evictionStrategy.size() >= maxSize) {
                CacheEntry<K, V> evicted = evictionStrategy.evict();
                if (evicted != null) {
                    map.remove(evicted.getKey());
                    // evicted entry is now unreferenced → eligible for GC
                }
            }

            // Create new entry via factory (DefaultEntryFactory or LFUEntryFactory)
            CacheEntry<K, V> newEntry = entryFactory.create(key, value, ttlMs);
            map.put(key, newEntry);
            evictionStrategy.onInsert(newEntry);

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Removes a key from the cache.
     *
     * Returns true if the key existed and was removed.
     * Returns false if the key was not present (idempotent).
     *
     * Both map and eviction strategy are cleaned up atomically
     * under the write lock. No partial state possible.
     *
     * @return true if key was present and removed, false if not found
     */
    public boolean delete(K key) {
        writeLock.lock();
        try {
            CacheEntry<K, V> entry = map.remove(key);
            if (entry == null) return false;  // key was not present

            evictionStrategy.onRemove(entry);
            return true;

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Scans all entries and removes those whose TTL has expired.
     * Called periodically by TTLReaper on a background thread.
     *
     * WHY THIS IS NEEDED alongside lazy expiration in get():
     *   A key inserted with TTL but never read again would live in
     *   memory forever without this scan. The reaper reclaims that
     *   memory proactively.
     *
     * TWO-PHASE APPROACH (collect then remove):
     *   Phase 1: iterate map, collect expired keys into a list.
     *   Phase 2: iterate the list, remove each from map + strategy.
     *
     *   Why not remove during iteration?
     *   ConcurrentHashMap's iterator allows removal via iterator.remove()
     *   during iteration — that would work. But we also need to call
     *   evictionStrategy.onRemove() for each expired entry.
     *   Mixing iteration + two-structure removal is harder to read.
     *   Two-phase is clearer: collect first, clean up second.
     *
     * PERFORMANCE NOTE:
     *   This holds the write lock for the entire scan.
     *   For a partition with 10,000 entries and 100 expired:
     *     scan 10,000 entries × ~10ns each ≈ 100µs total
     *   This blocks all get/put on this partition for ~100µs.
     *   At 1-second reaper intervals this is 0.01% blocking time.
     *   Acceptable. If not: scan without lock, re-check with lock before removing.
     *
     * @return number of entries expired and removed in this scan
     */
    public int expireStaleEntries() {
        writeLock.lock();
        try {
            // Phase 1: collect expired entries without removing
            List<CacheEntry<K, V>> expired = new ArrayList<>();
            for (CacheEntry<K, V> entry : map.values()) {
                if (entry.isExpired()) {
                    expired.add(entry);
                }
            }

            // Phase 2: remove from both structures
            for (CacheEntry<K, V> entry : expired) {
                map.remove(entry.getKey());
                evictionStrategy.onRemove(entry);
            }

            return expired.size();

        } finally {
            writeLock.unlock();
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Diagnostic methods
    // ─────────────────────────────────────────────────────────────

    /**
     * Returns current number of entries in this partition.
     * Uses readLock — does not mutate anything.
     */
    public int size() {
        readLock.lock();
        try {
            return map.size();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns max capacity of this partition.
     * maxSize is final — no lock needed. Safe publication via final field.
     */
    public int getMaxSize() {
        return maxSize;
    }
}