package org.cache.core;

/**
 * The atomic memory unit of the cache.
 *
 * Owns: key identity, value payload, TTL metadata, DLL pointers.
 * Does NOT own: eviction-strategy-specific metadata (frequency, score, etc.)
 * That metadata lives in subclasses (LFUCacheEntry) so LRU entries
 * never pay memory cost for fields they never use.
 *
 * ─────────────────────────────────────────────────────────────────
 * MEMORY LAYOUT  (64-bit JVM, compressed oops, heap < 32GB)
 * ─────────────────────────────────────────────────────────────────
 *  Object header    12 bytes  (mark word 8 + compressed class ptr 4)
 *  key    (ref)      4 bytes  → points to K object on heap
 *  value  (ref)      4 bytes  → points to V object on heap
 *  ttlMs  (long)     8 bytes  primitive, inline
 *  createdAt (long)  8 bytes  primitive, inline
 *  prev   (ref)      4 bytes  → adjacent CacheEntry or null
 *  next   (ref)      4 bytes  → adjacent CacheEntry or null
 *  padding           4 bytes  JVM aligns to 8-byte boundary
 * ─────────────────────────────────────────────────────────────────
 *  Shell total:     48 bytes  (not counting K and V objects)
 *
 *  At 1,000,000 entries:
 *    Shells:      48 MB
 *    String keys: ~52 MB  (avg 12-char key)
 *    byte[] vals: depends on payload size
 * ─────────────────────────────────────────────────────────────────
 *
 * ─────────────────────────────────────────────────────────────────
 * THREAD SAFETY CONTRACT
 * ─────────────────────────────────────────────────────────────────
 *  key       final   → safely published by JMM after construction.
 *                      Any thread can read without a lock.
 *
 *  ttlMs     final   → same guarantee as key.
 *
 *  createdAt final   → same guarantee as key.
 *
 *  value     mutable → NOT volatile. Visibility provided by
 *                      CachePartition's ReentrantReadWriteLock.
 *                      WRITE: must hold partition write lock.
 *                      READ:  must hold partition read or write lock.
 *                      Reading without any lock = data race.
 *
 *  prev/next mutable → NOT volatile. Must hold partition WRITE lock
 *                      for ALL access (reads and writes).
 *                      DLL rewiring is a 6-step non-atomic operation.
 *                      Any intermediate state is a broken list.
 *                      The write lock makes all 6 steps appear atomic.
 * ─────────────────────────────────────────────────────────────────
 *
 * WHY NOT A JAVA RECORD?
 *   Records make all fields final (shallowly immutable).
 *   prev/next must be mutated by LRUEvictionStrategy on every get().
 *   Records cannot support mutable fields. Plain class is required.
 *
 * WHY GENERIC <K, V> AND NOT byte[] VALUE?
 *   byte[] (Redis model): type-agnostic, caller serializes before storing.
 *   Generic V: compile-time type safety, no casting.
 *   We use generic V here for correctness guarantees during development.
 *   A production Redis-style cache would use byte[] for universality.
 *
 * @param <K> Key type. Must implement hashCode() and equals() correctly.
 *            Broken equals() causes silent lookup failures in HashMap.
 *            String is the safe default.
 * @param <V> Value type. null is a valid value (negative caching — caching
 *            the fact that a key does not exist in the backing store).
 */
public class CacheEntry<K, V> {

    /**
     * The identity of this cache entry.
     *
     * final: The HashMap bucket this entry lives in was chosen by
     * key.hashCode() at insertion. If key could change post-insertion,
     * the entry would be in the wrong bucket — findable never again.
     * Immutability here is a correctness requirement, not just style.
     *
     * package-private: LRUEvictionStrategy and LFUEvictionStrategy
     * (same package) access this directly on the hot eviction path.
     * Outside callers use getKey().
     *
     * Thread safety: final → safely published. Zero-cost reads from
     * any thread after construction completes.
     */
    final K key;

    /**
     * The payload stored by the caller.
     *
     * NOT final: put() on an existing key updates value in place,
     * avoiding a new CacheEntry allocation + GC + DLL re-link.
     * At 100K writes/sec, that is 100K avoided allocations/sec.
     *
     * NOT volatile: a memory fence on every read (the hottest path)
     * would hurt throughput measurably. Visibility is provided by
     * CachePartition's ReentrantReadWriteLock instead — the lock's
     * unlock() acts as a memory fence that makes writes visible to
     * any thread that subsequently acquires the lock.
     *
     * null is valid: supports negative caching.
     *
     * Thread safety: see class-level contract above.
     */
    V value;

    /**
     * How long this entry is allowed to live, in milliseconds.
     *
     * Stores DURATION, not deadline (createdAt + ttlMs).
     * Reason: (key, value, createdAt, ttlMs) is a complete record.
     * Storing only expiresAt loses the original duration — information
     * lost forever, matters for serialization and replication.
     *
     * 0 or negative: no expiration. Entry lives until manually evicted
     * or capacity-evicted.
     *
     * final: TTL is immutable. Mutable TTL creates races with the
     * TTL reaper — the reaper could decide "not expired" on a stale
     * ttlMs value just before another thread lowers it.
     *
     * Thread safety: final primitive. Safely published. No lock needed.
     */
    final long ttlMs;

    /**
     * Epoch milliseconds at construction time (System.currentTimeMillis()).
     * Used by isExpired() to compute: (now - createdAt) > ttlMs.
     *
     * Why currentTimeMillis() not nanoTime()?
     *   nanoTime() is monotonic — immune to NTP clock adjustments.
     *   currentTimeMillis() can jump backward during NTP sync, causing
     *   entries to appear younger than they are (missed expirations).
     *   For this project: currentTimeMillis() is acceptable.
     *   For production multi-node: prefer nanoTime() for TTL precision.
     *
     * final: written once. Safely published. No lock needed.
     */
    final long createdAt;

    /**
     * Doubly-linked list pointers for eviction ordering (LRU).
     *
     * package-private: only eviction strategies in this package
     * manipulate them. Direct field access = zero accessor overhead
     * on the hottest path in the cache.
     *
     * NOT final: rewired on every cache read (moveToHead) and every
     * eviction (unlinkNode). These are the most frequently mutated
     * fields in the system.
     *
     * NOT volatile: see value field reasoning. Lock provides visibility.
     *
     * Null convention:
     *   prev == null → this entry is NOT linked in any eviction list.
     *   Occurs: brand-new entry before onInsert(), or after onRemove().
     *   LRUEvictionStrategy uses this to detect unlinked state.
     *
     * Thread safety: MUST hold partition WRITE lock for ALL access.
     *
     * Why write lock even for reads of prev/next?
     * moveToHead() is 6 non-atomic pointer mutations. A thread reading
     * prev/next during those 6 steps sees a partially-broken list.
     * The write lock makes all 6 steps appear instantaneous to observers.
     * There is no safe "read-only" observation of a DLL mid-rewire.
     */
    CacheEntry<K, V> prev;
    CacheEntry<K, V> next;

    /**
     * Constructs a real cache entry.
     *
     * After this constructor returns, all final fields are guaranteed
     * visible to any thread — JMM safe publication via final fields.
     *
     * @param key    must be non-null; identity of this entry
     * @param value  may be null (negative caching)
     * @param ttlMs  0 or negative for no expiration; positive for TTL in ms
     */
    public CacheEntry(K key, V value, long ttlMs) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        this.key = key;
        this.value = value;
        this.ttlMs = ttlMs;
        this.createdAt = System.currentTimeMillis();
    }

    /**
     * Protected no-arg constructor for sentinel nodes only.
     *
     * Sentinel nodes (dummyHead, dummyTail in LRUEvictionStrategy)
     * need to exist as CacheEntry instances but have no real key or value.
     * They exist solely to eliminate null checks at DLL boundaries.
     *
     * protected: only SentinelEntry subclass (same package) can call this.
     * No outside code can accidentally create a keyless CacheEntry.
     *
     * Intentionally bypasses the null check — sentinels have null keys
     * by design. They are never looked up, never evicted, never returned
     * to callers.
     */
    protected CacheEntry() {
        this.key = null;
        this.value = null;
        this.ttlMs = 0;
        this.createdAt = 0;
    }

    /**
     * Returns true if this entry has lived beyond its TTL.
     *
     * Called in two places:
     *
     * 1. LAZILY in CachePartition.get():
     *    Check on read. If expired, remove it then. No background thread.
     *    Pro: zero overhead when keys are not read.
     *    Con: expired entries occupy memory until their next read attempt.
     *
     * 2. EAGERLY by TTLReaper (background thread):
     *    Scans all partitions every N seconds, removes expired entries
     *    even if never read again.
     *    Pro: memory reclaimed promptly.
     *    Con: background thread holds write locks briefly during scan.
     *
     * Both run in this system — lazy expiration for read latency,
     * reaper for memory hygiene.
     *
     * Thread safety: reads only final fields (ttlMs, createdAt) and
     * System.currentTimeMillis() which is thread-safe. Safe to call
     * without any lock. Result may be stale by a few milliseconds but
     * never incorrect — an entry cannot un-expire.
     */
    public boolean isExpired() {
        if (ttlMs <= 0) return false;
        return (System.currentTimeMillis() - createdAt) > ttlMs;
    }

    public K    getKey()              { return key; }
    public V    getValue()            { return value; }
    public void setValue(V value)     { this.value = value; }
    public long getTtlMs()            { return ttlMs; }
    public long getCreatedAt()        { return createdAt; }
}