package org.cache.core;

/**
 * CacheEntry extended with frequency tracking for LFU eviction.
 *
 * Adds exactly two fields to CacheEntry:
 *   frequency      — decaying access counter (double, not int)
 *   lastAccessedAt — epoch ms of most recent access
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY SUBCLASS INSTEAD OF ADDING FREQUENCY TO CacheEntry?
 * ─────────────────────────────────────────────────────────────────
 * Memory: LRU entries never use frequency. At 10M entries, an int
 * frequency field wastes 40MB on entries that never read it.
 *
 * Design: CacheEntry is pure data — key, value, TTL, DLL pointers.
 * How an entry gets evicted is the strategy's concern, not the
 * entry's. Frequency is eviction-strategy-specific metadata.
 *
 * Extensibility: adding W-TinyLFU later would need a different
 * frequency model. Subclassing keeps CacheEntry stable.
 * ─────────────────────────────────────────────────────────────────
 *
 * ─────────────────────────────────────────────────────────────────
 * MEMORY LAYOUT  (64-bit JVM, compressed oops)
 * Inherited from CacheEntry:       48 bytes
 * + frequency   (double)            8 bytes  primitive
 * + lastAccessedAt (long)           8 bytes  primitive
 * ─────────────────────────────────────────────────────────────────
 * Total per LFUCacheEntry shell:   64 bytes
 * vs CacheEntry shell:             48 bytes
 * Overhead of LFU tracking:        16 bytes per entry
 *
 * At 1M entries: 16MB extra vs plain CacheEntry.
 * Worth it for correct LFU behavior. Not worth paying for LRU entries.
 * ─────────────────────────────────────────────────────────────────
 *
 * ─────────────────────────────────────────────────────────────────
 * FREQUENCY MODEL
 * ─────────────────────────────────────────────────────────────────
 * On every access:
 *   frequency = (frequency * decayFactor) + 1.0
 *
 * Why double not int?
 *   int truncates decay results — (int)(3 * 0.9) + 1 = 3, no change.
 *   Decay becomes completely ineffective for low-frequency entries.
 *   double preserves fractional precision that makes decay work.
 *
 * Why decay BEFORE incrementing?
 *   Decay represents aging of past accesses.
 *   The current access is happening now — should count at full weight.
 *   Decaying after increment would discount the current access too.
 *
 * Why start at 1.0?
 *   Prevents new entry starvation. A new entry at 0.0 would be
 *   instantly evicted over any existing entry at frequency >= 1.0,
 *   before it has a chance to prove its access pattern.
 *
 * Steady-state maximum (with decayFactor = 0.9):
 *   At steady state: freq = freq * 0.9 + 1.0
 *   Solving: freq * 0.1 = 1.0 → freq = 10.0
 *   A continuously-hot key stabilizes at exactly 10.0.
 *   Frequency is mathematically bounded — no unbounded growth.
 *   General formula: steady_state = 1.0 / (1.0 - decayFactor)
 *
 * ─────────────────────────────────────────────────────────────────
 * KNOWN LIMITATION — COLD KEY FREEZE
 * ─────────────────────────────────────────────────────────────────
 * Access-based decay only fires when the entry IS accessed.
 * A key that goes cold is never decayed again until next access.
 *
 * Example: "trending-post" accessed heavily → frequency = 9.8
 * Traffic stops. 10 minutes later: frequency still = 9.8
 * New entries at frequency = 1.0 will be evicted over this cold key.
 *
 * Mitigation: lastAccessedAt enables time-based decay as an extension.
 * At access time: compute idle time, apply time penalty to frequency.
 *   timePenalty = decayFactor ^ (idleSeconds)
 *   frequency = (frequency * timePenalty) + 1.0
 * This zero-penalty for hot keys, severe penalty for cold ones.
 * We store lastAccessedAt now for free so this extension requires
 * only a change to LFUEvictionStrategy.onAccess(), not this class.
 *
 * ─────────────────────────────────────────────────────────────────
 * THREAD SAFETY CONTRACT (inherits from CacheEntry, adds:)
 * ─────────────────────────────────────────────────────────────────
 * frequency      mutable, NOT volatile.
 *                WRITE: must hold partition write lock.
 *                READ:  must hold partition read or write lock.
 *
 * lastAccessedAt mutable, NOT volatile. Same contract as frequency.
 * ─────────────────────────────────────────────────────────────────
 */
public class LFUCacheEntry<K, V> extends CacheEntry<K, V> {

    /**
     * Access frequency as a decaying double counter.
     *
     * double (not int): int truncation destroys decay effectiveness.
     * See class-level Javadoc for full proof.
     *
     * Starts at 1.0: prevents new-entry starvation.
     * Steady state at decayFactor=0.9: 10.0 (mathematically bounded).
     *
     * package-private: LFUEvictionStrategy (same package) mutates
     * this directly on every access — no getter overhead on hot path.
     *
     * Thread safety: must hold partition write lock for read and write.
     */
    double frequency;

    /**
     * Epoch milliseconds of the most recent access.
     * Updated by LFUEvictionStrategy.onAccess() on every cache read.
     *
     * Why long not int?
     * Current epoch ms ≈ 1,700,000,000,000 — far beyond int max of
     * 2,147,483,647. int timestamps overflow in 2038 (Y2038 problem).
     * Always use long for epoch milliseconds.
     *
     * Enables three things:
     * 1. Observability: idleMs = now - lastAccessedAt
     * 2. Time-based decay extension (see class-level Javadoc)
     * 3. Tie-breaking within same frequency bucket (alternative to
     *    LinkedHashSet insertion-order tiebreaking)
     *
     * package-private: LFUEvictionStrategy accesses directly.
     *
     * Thread safety: must hold partition write lock for read and write.
     */
    long lastAccessedAt;

    /**
     * Constructs a new LFU cache entry.
     * frequency starts at 1.0 (not 0.0) to prevent new-entry starvation.
     * lastAccessedAt = createdAt (first "access" is the insertion itself).
     */
    public LFUCacheEntry(K key, V value, long ttlMs) {
        super(key, value, ttlMs);
        this.frequency = 1.0;
        this.lastAccessedAt = this.createdAt;
    }

    /**
     * Protected no-arg constructor inherited from CacheEntry.
     * Available but unused in LFU — LFUEvictionStrategy has no DLL
     * and therefore needs no sentinel nodes.
     * Documented here to explain why it's not called.
     */
    protected LFUCacheEntry() {
        super();
        this.frequency = 0.0;
        this.lastAccessedAt = 0;
    }

    public double getFrequency()      { return frequency; }
    public long   getLastAccessedAt() { return lastAccessedAt; }
}