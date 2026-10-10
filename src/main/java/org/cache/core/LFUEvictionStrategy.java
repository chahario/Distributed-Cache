package org.cache.core;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;

/**
 * Least Frequently Used eviction strategy with access-based frequency decay.
 *
 * ─────────────────────────────────────────────────────────────────
 * CORE DATA STRUCTURES
 * ─────────────────────────────────────────────────────────────────
 *
 * freqBuckets: TreeMap<Integer, LinkedHashSet<K>>
 *   Maps integer frequency bucket → set of keys at that frequency.
 *   TreeMap: keeps buckets sorted → firstKey() = minimum frequency.
 *   LinkedHashSet: insertion-order iteration → oldest key evicted first
 *   within the same bucket (LRU tiebreaker).
 *
 *   Example state with 5 entries:
 *     Bucket 1: [keyE, keyF]   ← lowest freq, eviction target
 *     Bucket 3: [keyB, keyD]
 *     Bucket 9: [keyA, keyC]   ← hottest entries
 *
 * keyToBucket: HashMap<K, Integer>
 *   Maps key → current integer bucket index.
 *   Used in onAccess to find old bucket before re-bucketing.
 *   Updated whenever an entry moves between buckets.
 *
 * entryIndex: HashMap<K, LFUCacheEntry<K,V>>
 *   Maps key → entry object.
 *   Required by evict(): gets a key from freqBuckets, needs the entry.
 *   Not needed for onAccess/onRemove — those receive the entry directly.
 *
 * ─────────────────────────────────────────────────────────────────
 * TIME COMPLEXITY
 * ─────────────────────────────────────────────────────────────────
 *   onInsert  O(1)       add to bucket 1, update two maps
 *   onAccess  O(log b)   b = number of distinct buckets
 *                        In practice b <= 10 (decay bounds frequency)
 *                        Effectively O(1) for realistic workloads
 *   onRemove  O(log b)   same reasoning
 *   evict()   O(log b)   TreeMap.firstKey() + LinkedHashSet.iterator()
 *   size()    O(1)       explicit int field
 *
 * ─────────────────────────────────────────────────────────────────
 * DECAY MODEL
 * ─────────────────────────────────────────────────────────────────
 *   On every access:
 *     entry.frequency = (entry.frequency * decayFactor) + 1.0
 *
 *   Decay fires BEFORE increment — past accesses lose weight, this
 *   access counts at full value.
 *
 *   Steady-state maximum:
 *     freq = freq * d + 1  →  freq(1-d) = 1  →  freq = 1/(1-d)
 *     decayFactor=0.90 → max freq = 10.0
 *     decayFactor=0.95 → max freq = 20.0
 *     decayFactor=0.50 → max freq =  2.0
 *
 *   Number of distinct buckets ≈ floor(1/(1-decayFactor))
 *   With decayFactor=0.9: at most ~10 buckets.
 *   This is why TreeMap overhead is negligible in practice.
 *
 * ─────────────────────────────────────────────────────────────────
 * KNOWN LIMITATION — COLD KEY FREEZE
 * ─────────────────────────────────────────────────────────────────
 *   Access-based decay only fires when the entry IS accessed.
 *   Cold keys (never accessed again) retain their last frequency forever.
 *   They will only be evicted when all other entries have higher frequency.
 *
 *   Mitigation: lastAccessedAt in LFUCacheEntry enables time-based decay
 *   as a future extension — apply idle-time penalty at access time.
 *   No structural change to this class needed, only onAccess() logic.
 *
 * ─────────────────────────────────────────────────────────────────
 * THREAD SAFETY
 * ─────────────────────────────────────────────────────────────────
 *   NOT thread-safe. Intentional — same reasoning as LRUEvictionStrategy.
 *   ALL methods require caller (CachePartition) to hold partition
 *   WRITE lock before calling.
 *
 *   Why write lock for onAccess?
 *   onAccess mutates: entry.frequency, entry.lastAccessedAt, keyToBucket,
 *   freqBuckets (possibly two buckets), entryIndex.
 *   Five data structures mutated. Must appear atomic to other threads.
 */
public class LFUEvictionStrategy<K, V> implements EvictionStrategy<K, V> {

    // ─────────────────────────────────────────────────────────────
    // Configuration
    // ─────────────────────────────────────────────────────────────

    /**
     * Default decay factor applied on every access.
     * 0.9 means: each access, old frequency retains 90% of its weight.
     *
     * Lower = more aggressive decay (recent accesses dominate strongly).
     * Higher = slower decay (history matters longer).
     *
     * 0.9 gives steady-state max of 10.0 — clean, bounded, predictable.
     *
     * Must be strictly between 0.0 and 1.0:
     *   0.0 → frequency always resets to 1.0 on every access (no history)
     *   1.0 → no decay ever (pure LFU, no aging — frequency pollution returns)
     *   Both extremes defeat the purpose of decay.
     */
    static final double DEFAULT_DECAY_FACTOR = 0.9;

    private final double decayFactor;

    // ─────────────────────────────────────────────────────────────
    // Core data structures
    // ─────────────────────────────────────────────────────────────

    /**
     * Maps integer frequency bucket → ordered set of keys at that frequency.
     *
     * TreeMap: automatic ascending sort → firstKey() = minimum bucket.
     * This gives us O(log b) eviction target without manual minFreq tracking.
     *
     * LinkedHashSet per bucket: insertion-order preserved → among entries
     * with the same frequency, oldest inserted is evicted first.
     * This is the LRU tiebreaker — same policy Redis LFU uses.
     *
     * Empty buckets are removed immediately in removeFromBucket().
     * This keeps freqBuckets.size() == number of DISTINCT frequencies.
     * Without cleanup, firstKey() could return an empty bucket.
     */
    private final TreeMap<Integer, LinkedHashSet<K>> freqBuckets;

    /**
     * Maps key → current integer bucket index.
     *
     * Why store this separately instead of computing floor(entry.frequency)?
     * In onAccess, we apply decay THEN compute new bucket. But we need
     * the OLD bucket (before decay) to remove the entry from it.
     * If we only had entry.frequency, we'd need to un-apply decay to find
     * the old bucket — fragile and incorrect.
     * Explicit map: always O(1) accurate, no order-of-operations dependency.
     *
     * Updated atomically with freqBuckets in onInsert, onAccess, onRemove.
     */
    private final Map<K, Integer> keyToBucket;

    /**
     * Maps key → LFUCacheEntry object.
     *
     * Required by evict(): the eviction target is identified as a KEY
     * from freqBuckets, but CachePartition needs the ENTRY object returned.
     * Without this map, evict() has no way to look up the entry by key.
     *
     * onAccess and onRemove receive the entry directly — they don't use this.
     * Only evict() needs it.
     *
     * This is a second index alongside CachePartition's ConcurrentHashMap.
     * Memory cost: one extra HashMap reference per entry.
     * Worth it to keep LFUEvictionStrategy self-contained and not coupled
     * to CachePartition's internal map.
     */
    private final Map<K, LFUCacheEntry<K, V>> entryIndex;

    /**
     * Number of real entries currently tracked.
     * Maintained explicitly for O(1) size().
     */
    private int size;

    // ─────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────

    public LFUEvictionStrategy() {
        this(DEFAULT_DECAY_FACTOR);
    }

    /**
     * @param decayFactor must be strictly between 0.0 and 1.0 exclusive.
     *                    0.9 is a good default for most workloads.
     */
    public LFUEvictionStrategy(double decayFactor) {
        if (decayFactor <= 0.0 || decayFactor >= 1.0) {
            throw new IllegalArgumentException(
                    "decayFactor must be in (0.0, 1.0) exclusive. " +
                            "0.0 = no history (always resets), 1.0 = no decay (frequency pollution). " +
                            "Got: " + decayFactor
            );
        }
        this.decayFactor = decayFactor;
        this.freqBuckets = new TreeMap<>();
        this.keyToBucket = new HashMap<>();
        this.entryIndex  = new HashMap<>();
        this.size = 0;
    }

    // ─────────────────────────────────────────────────────────────
    // EvictionStrategy contract
    // ─────────────────────────────────────────────────────────────

    /**
     * Tracks a brand-new entry at frequency bucket 1.
     *
     * New entries always start at frequency 1.0 → bucket 1.
     * This is set in LFUCacheEntry constructor — we don't set it here.
     * We trust the entry arrives with frequency=1.0.
     *
     * Why bucket 1 and not 0?
     *   Bucket 0 would mean the entry is immediately the lowest-priority
     *   entry in the cache — evicted before anything else on the next put.
     *   Starting at 1 gives the entry one "vote" — it won't outlast a hot
     *   entry at bucket 5, but it won't be instantly murdered either.
     *
     * Three structures updated atomically (under caller's write lock):
     *   1. freqBuckets: add key to bucket 1
     *   2. keyToBucket: record key → bucket 1
     *   3. entryIndex:  record key → entry
     *
     * CALLER MUST HOLD: partition write lock.
     *
     * @throws IllegalArgumentException if entry is null or not LFUCacheEntry
     * @throws IllegalStateException    if entry is already tracked
     */
    @Override
    public void onInsert(CacheEntry<K, V> entry) {
        LFUCacheEntry<K, V> lfuEntry = castToLFU(entry);

        if (keyToBucket.containsKey(lfuEntry.key)) {
            throw new IllegalStateException(
                    "onInsert called on already-tracked key: " + lfuEntry.key +
                            ". Use onAccess() for existing entries."
            );
        }

        // LFUCacheEntry constructor sets frequency=1.0, lastAccessedAt=createdAt
        // We trust that and use bucket 1 directly
        int bucket = toBucket(lfuEntry.frequency);  // = 1

        addToBucket(lfuEntry.key, bucket);
        keyToBucket.put(lfuEntry.key, bucket);
        entryIndex.put(lfuEntry.key, lfuEntry);
        size++;
    }

    /**
     * Applies decay, increments frequency, re-buckets if needed.
     *
     * This is the core of LFU with decay. Four things happen:
     *
     * Step 1: Apply decay to raw frequency
     *   entry.frequency *= decayFactor
     *   Old accesses lose weight. Hot yesterday = less hot today.
     *
     * Step 2: Increment for this access
     *   entry.frequency += 1.0
     *   This access counts at full weight (not decayed).
     *
     * Step 3: Update lastAccessedAt
     *   Enables idle-time detection and future time-based decay.
     *
     * Step 4: Re-bucket if integer bucket changed
     *   Compute new bucket = floor(new frequency)
     *   If different from old bucket: move key between buckets.
     *   If same: no structural change needed (saves LinkedHashSet churn).
     *
     * Example walkthrough (decayFactor=0.9):
     *   entry in bucket 3, frequency=3.7
     *   Step 1: 3.7 * 0.9 = 3.33
     *   Step 2: 3.33 + 1.0 = 4.33
     *   Step 3: lastAccessedAt = now
     *   Step 4: new bucket = floor(4.33) = 4, old = 3 → re-bucket
     *
     *   entry in bucket 9, frequency=9.6  (near steady state)
     *   Step 1: 9.6 * 0.9 = 8.64
     *   Step 2: 8.64 + 1.0 = 9.64
     *   Step 4: new bucket = floor(9.64) = 9, old = 9 → no re-bucket
     *   Hot key stays in high bucket with minimal overhead.
     *
     * CALLER MUST HOLD: partition write lock.
     *
     * @throws IllegalArgumentException if entry is not LFUCacheEntry
     * @throws IllegalStateException    if entry is not currently tracked
     */
    @Override
    public void onAccess(CacheEntry<K, V> entry) {
        LFUCacheEntry<K, V> lfuEntry = castToLFU(entry);

        Integer oldBucket = keyToBucket.get(lfuEntry.key);
        if (oldBucket == null) {
            throw new IllegalStateException(
                    "onAccess called on untracked key: " + lfuEntry.key +
                            ". Was this entry ever inserted? Or was it already evicted?"
            );
        }

        // Step 1 + 2: decay then increment (order matters — see LFUCacheEntry)
        lfuEntry.frequency = (lfuEntry.frequency * decayFactor) + 1.0;

        // Step 3: update access timestamp
        lfuEntry.lastAccessedAt = System.currentTimeMillis();

        // Step 4: re-bucket only if integer bucket changed
        int newBucket = toBucket(lfuEntry.frequency);
        if (newBucket != oldBucket) {
            removeFromBucket(lfuEntry.key, oldBucket);
            addToBucket(lfuEntry.key, newBucket);
            keyToBucket.put(lfuEntry.key, newBucket);
        }
        // If same bucket: LinkedHashSet already has the key — no change needed.
        // Note: position within LinkedHashSet does NOT change on access.
        // LinkedHashSet tracks insertion order, not access order.
        // Within a bucket, tiebreaking is "oldest inserted" not "least recently used".
        // This is intentional — true LFU tiebreaker. If you want LRU tiebreaker
        // within a bucket: remove + re-add the key to push it to the end.
    }

    /**
     * Removes an entry from all tracking structures.
     *
     * Called in two contexts:
     * 1. CachePartition.delete() — explicit removal by caller.
     * 2. CachePartition.get()   — lazy TTL expiration on read.
     *
     * Idempotent: if key is not in keyToBucket, returns silently.
     * Handles double-remove safely — TTL reaper and lazy expiration
     * may both attempt to remove the same entry.
     *
     * Three structures cleaned up atomically:
     *   1. freqBuckets: remove key from its bucket (delete bucket if empty)
     *   2. keyToBucket: remove key mapping
     *   3. entryIndex:  remove key → entry mapping
     *
     * CALLER MUST HOLD: partition write lock.
     */
    @Override
    public void onRemove(CacheEntry<K, V> entry) {
        LFUCacheEntry<K, V> lfuEntry = castToLFU(entry);

        Integer bucket = keyToBucket.remove(lfuEntry.key);
        if (bucket == null) {
            // Already removed — idempotent, not an error
            return;
        }

        removeFromBucket(lfuEntry.key, bucket);
        entryIndex.remove(lfuEntry.key);
        size--;
    }

    /**
     * Evicts the entry in the lowest frequency bucket.
     * Among ties (same bucket), evicts the oldest inserted entry
     * (first in LinkedHashSet insertion order).
     *
     * Returns the evicted CacheEntry so CachePartition can:
     *   1. Remove it from the primary ConcurrentHashMap
     *   2. Log it, emit a metric, notify an eviction listener
     *
     * This method handles ONLY the tracking structures (freqBuckets,
     * keyToBucket, entryIndex). It does NOT touch the primary map.
     * Single responsibility — strategy manages ordering, partition
     * manages the primary index.
     *
     * Eviction path visualized:
     *
     *   freqBuckets:
     *     Bucket 1: [keyE, keyF]   ← firstKey() returns 1
     *     Bucket 3: [keyB, keyD]
     *
     *   iterator().next() on bucket 1's LinkedHashSet → keyE (oldest)
     *   → look up keyE in entryIndex → get LFUCacheEntry
     *   → call onRemove(entry) → clean up all three structures
     *   → return entry to CachePartition
     *
     * Returns null if no entries are tracked.
     *
     * CALLER MUST HOLD: partition write lock.
     */
    @Override
    public CacheEntry<K, V> evict() {
        if (size == 0) return null;

        // TreeMap.firstEntry() — O(log b), b = number of distinct buckets
        // In practice b <= 10 with decayFactor=0.9, so effectively O(1)
        Map.Entry<Integer, LinkedHashSet<K>> minBucketEntry =
                freqBuckets.firstEntry();

        if (minBucketEntry == null) {
            // size > 0 but freqBuckets is empty — accounting bug
            throw new IllegalStateException(
                    "evict() found empty freqBuckets but size=" + size +
                            ". Tracking structures are inconsistent. " +
                            "Check onInsert/onRemove pairing."
            );
        }

        LinkedHashSet<K> minBucket = minBucketEntry.getValue();

        // LinkedHashSet iterator returns insertion order — first = oldest
        K victimKey = minBucket.iterator().next();

        // entryIndex lookup — O(1)
        LFUCacheEntry<K, V> victim = entryIndex.get(victimKey);

        // Reuse onRemove for cleanup — single source of truth for removal logic
        onRemove(victim);

        return victim;
    }

    /**
     * Returns number of tracked entries. O(1).
     * CALLER MUST HOLD: partition read or write lock.
     */
    @Override
    public int size() {
        return size;
    }

    // ─────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────

    /**
     * Converts raw double frequency to integer bucket index.
     *
     * floor(): freq 3.9 → bucket 3, not bucket 4.
     * We don't promote an entry to a higher bucket until it has
     * genuinely crossed the integer threshold. Conservative promotion
     * means entries spend more time in lower buckets → more eviction
     * pressure on cold entries. Correct behavior.
     *
     * Math.max(1, ...): ensures bucket is never 0 or negative.
     * Bucket 0 is reserved conceptually for "not yet inserted".
     * A decayed frequency of 0.7 → floor = 0 → we clamp to 1.
     * This prevents newly-decayed entries from falling below new entries.
     *
     * Example values:
     *   frequency=1.0  → bucket 1  (new entry)
     *   frequency=1.9  → bucket 1  (still in entry bucket)
     *   frequency=2.0  → bucket 2  (crossed threshold)
     *   frequency=9.82 → bucket 9  (near steady state)
     */
    private int toBucket(double frequency) {
        return Math.max(1, (int) Math.floor(frequency));
    }

    /**
     * Adds a key to the specified frequency bucket.
     * Creates the bucket (LinkedHashSet) if it doesn't exist.
     *
     * computeIfAbsent: atomic check-then-insert on the TreeMap.
     * If bucket doesn't exist, creates a new LinkedHashSet.
     * Then adds the key to the (existing or new) set.
     */
    private void addToBucket(K key, int bucket) {
        freqBuckets
                .computeIfAbsent(bucket, b -> new LinkedHashSet<>())
                .add(key);
    }

    /**
     * Removes a key from the specified frequency bucket.
     * Deletes the bucket entirely if it becomes empty.
     *
     * Why delete empty buckets?
     * If we keep empty buckets, freqBuckets.firstKey() might return
     * a bucket with no entries. evict() would find an empty LinkedHashSet
     * and iterator().next() would throw NoSuchElementException.
     * Deleting empty buckets immediately keeps firstKey() always valid.
     *
     * Why is this safe even if the bucket doesn't exist?
     * Early return on null — idempotent, no crash.
     */
    private void removeFromBucket(K key, int bucket) {
        LinkedHashSet<K> bucketSet = freqBuckets.get(bucket);
        if (bucketSet == null) return;

        bucketSet.remove(key);

        if (bucketSet.isEmpty()) {
            freqBuckets.remove(bucket);
        }
    }

    /**
     * Safe downcast from CacheEntry to LFUCacheEntry.
     *
     * Why not just cast directly?
     * If CachePartition is misconfigured — using LFUEvictionStrategy
     * with DefaultEntryFactory (creates plain CacheEntry, not LFUCacheEntry)
     * — the cast would throw a cryptic ClassCastException deep in the code.
     *
     * This method throws immediately with a clear message explaining
     * exactly what went wrong and how to fix it. Fail fast, fail clearly.
     *
     * This is a wiring error, not a runtime data error — it should be
     * caught in tests, not in production. But clear error messages
     * make the development loop faster.
     */
    private LFUCacheEntry<K, V> castToLFU(CacheEntry<K, V> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry cannot be null");
        }
        if (!(entry instanceof LFUCacheEntry)) {
            throw new IllegalArgumentException(
                    "LFUEvictionStrategy requires LFUCacheEntry instances. " +
                            "Received: " + entry.getClass().getSimpleName() + ". " +
                            "Fix: use LFUEntryFactory (not DefaultEntryFactory) when " +
                            "constructing CachePartition with LFUEvictionStrategy."
            );
        }
        return (LFUCacheEntry<K, V>) entry;
    }
}