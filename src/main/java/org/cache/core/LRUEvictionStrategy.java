package org.cache.core;

/**
 * Least Recently Used eviction strategy using a doubly-linked list.
 *
 * DATA STRUCTURE
 * ─────────────────────────────────────────────────────────────────
 * A doubly-linked list with two sentinel nodes:
 *
 *   dummyHead <──► [MRU] <──► ... <──► [LRU] <──► dummyTail
 *
 *   MRU = Most Recently Used = dummyHead.next
 *   LRU = Least Recently Used = dummyTail.prev  ← eviction target
 *
 * Why sentinels (dummyHead, dummyTail)?
 *   Without them, addToHead() and removeTail() need null checks for
 *   the empty-list case. With them, the list is never truly empty —
 *   always at least dummyHead <──► dummyTail. Every operation has
 *   exactly one code path. No special cases. Cleaner, safer code.
 *
 * ─────────────────────────────────────────────────────────────────
 * TIME COMPLEXITY
 * ─────────────────────────────────────────────────────────────────
 *   onInsert  O(1)  addToHead: 4 pointer writes
 *   onAccess  O(1)  unlinkNode (4 ptr writes) + addToHead (4 ptr writes)
 *   onRemove  O(1)  unlinkNode: 4 pointer writes
 *   evict()   O(1)  read dummyTail.prev + unlinkNode
 *   size()    O(1)  return int field
 *
 * ─────────────────────────────────────────────────────────────────
 * THREAD SAFETY
 * ─────────────────────────────────────────────────────────────────
 * NOT thread-safe. This is intentional — see reasoning below.
 *
 * ALL methods require the caller (CachePartition) to hold the
 * partition's ReentrantReadWriteLock WRITE lock before calling.
 * This includes onAccess() which appears read-like but mutates the DLL.
 *
 * Why not lock internally?
 *   CachePartition.get() must atomically: look up the map AND update
 *   the DLL. These two operations must be under ONE lock. If the DLL
 *   had its own lock, you'd need two locks simultaneously → deadlock risk.
 *   One external write lock covers both map and DLL. Simpler, safer.
 *
 * Why write lock even for onAccess (which appears read-like)?
 *   onAccess calls moveToHead which rewires 6 pointers in 6 non-atomic
 *   steps. A concurrent reader observing the DLL mid-rewire sees a
 *   broken list. prev/next are not volatile — no safe "read-only" view
 *   exists during mutation. Write lock required for all DLL access.
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY SEPARATE STRATEGY CLASS AND NOT INSIDE CachePartition?
 * ─────────────────────────────────────────────────────────────────
 * Strategy pattern: CachePartition doesn't care HOW eviction works,
 * only that it can call onInsert/onAccess/onRemove/evict().
 * Swapping to LFU requires changing one constructor argument, not
 * rewriting CachePartition. Open/Closed principle.
 */
public class LRUEvictionStrategy<K, V> implements EvictionStrategy<K, V> {

    /**
     * Sentinel node marking the MRU boundary.
     * dummyHead.next is always the Most Recently Used real entry.
     * dummyHead itself is never returned, never evicted, never exposed.
     *
     * Why a subclass (SentinelEntry) instead of a plain CacheEntry?
     * CacheEntry's public constructor requires a non-null key.
     * Sentinels have no meaningful key — they're structural placeholders.
     * SentinelEntry uses the protected no-arg constructor from CacheEntry
     * which intentionally bypasses the null check.
     */
    private final CacheEntry<K, V> dummyHead;

    /**
     * Sentinel node marking the LRU boundary.
     * dummyTail.prev is always the Least Recently Used real entry.
     * This is the eviction target — evict() reads dummyTail.prev.
     */
    private final CacheEntry<K, V> dummyTail;

    /**
     * Number of real entries currently tracked in this list.
     * Does NOT count the two sentinel nodes.
     *
     * Maintained explicitly for O(1) size(). Traversing the list
     * for count would be O(n) — unacceptable.
     *
     * Discipline: every addToHead increments, every unlinkNode
     * in a remove/evict context decrements. moveToHead does neither
     * (count unchanged). Getting this wrong causes silent bugs where
     * evict() returns null despite the list having entries.
     */
    private int size;

    // ─────────────────────────────────────────────────────────────
    // Sentinel node implementation
    // ─────────────────────────────────────────────────────────────

    /**
     * Sentinel subclass using CacheEntry's protected no-arg constructor.
     *
     * Why a nested private static class?
     *   static: doesn't need an outer class instance — no hidden reference.
     *   private: only LRUEvictionStrategy can create sentinels.
     *   Subclass: inherits CacheEntry's prev/next fields directly
     *             and uses the protected constructor to bypass null check.
     *
     * Why not just set key=null directly in LRUEvictionStrategy?
     *   CacheEntry's public constructor throws on null key.
     *   We need a constructor that allows null key for sentinels only.
     *   protected CacheEntry() in the parent class exists for exactly this.
     */
    private static class SentinelEntry<K, V> extends CacheEntry<K, V> {
        SentinelEntry() {
            super(); // protected no-arg: null key, no timestamp, no null check
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────

    /**
     * Initializes the DLL with two sentinels linked to each other.
     *
     * Initial state:
     *   dummyHead <──► dummyTail
     *             ◄──
     *
     * This represents an empty cache — no real entries, just the boundary.
     * size = 0 (sentinels are not counted).
     */
    public LRUEvictionStrategy() {
        this.dummyHead = new SentinelEntry<>();
        this.dummyTail = new SentinelEntry<>();

        // Establish the initial boundary
        dummyHead.next = dummyTail;
        dummyTail.prev = dummyHead;

        this.size = 0;
    }

    // ─────────────────────────────────────────────────────────────
    // EvictionStrategy contract implementation
    // ─────────────────────────────────────────────────────────────

    /**
     * Links a brand-new entry at the MRU position (after dummyHead).
     *
     * Called by CachePartition.put() when the key does NOT already exist.
     * NOT called on updates to existing keys — that goes through onAccess().
     *
     * Precondition: entry.prev == null (not yet linked anywhere).
     * Violation means the same entry is being inserted twice — caller bug.
     * We throw immediately rather than silently corrupting the DLL.
     *
     * CALLER MUST HOLD: partition write lock.
     *
     * @throws IllegalStateException if entry is already linked
     */
    @Override
    public void onInsert(CacheEntry<K, V> entry) {
        validateRealEntry(entry);

        if (entry.prev != null) {
            throw new IllegalStateException(
                    "onInsert called on an already-linked entry: key=" + entry.key +
                            ". This entry is already in the eviction list. " +
                            "Call onAccess() for existing entries."
            );
        }

        addToHead(entry);
        size++;
    }

    /**
     * Moves an existing entry to the MRU position.
     *
     * Called by CachePartition.put() when key EXISTS (value update in place)
     * and by CachePartition.get() on every successful cache read.
     *
     * Why get() must call onAccess:
     *   If we don't update LRU order on read, the eviction ordering reflects
     *   insertion order, not access order. We'd be evicting recently-read
     *   entries just because they were inserted early. That's not LRU — it's FIFO.
     *
     * Why this requires a WRITE lock even though it looks like a read:
     *   moveToHead rewires 6 pointers in 6 non-atomic steps.
     *   Any concurrent observer of the DLL mid-rewire sees a broken list.
     *   There is no safe "read-only" path through a DLL being mutated.
     *   The write lock makes all 6 steps appear instantaneous.
     *
     * Precondition: entry.prev != null (entry must be linked).
     * Calling onAccess on an unlinked entry means the entry was never
     * inserted, or was already removed — caller bug. Throw immediately.
     *
     * CALLER MUST HOLD: partition write lock.
     *
     * @throws IllegalArgumentException if entry is null or is a sentinel
     * @throws IllegalStateException    if entry is not currently linked
     */
    @Override
    public void onAccess(CacheEntry<K, V> entry) {
        validateRealEntry(entry);

        if (entry.prev == null) {
            throw new IllegalStateException(
                    "onAccess called on an unlinked entry: key=" + entry.key +
                            ". Entry is not currently in the eviction list. " +
                            "Was this entry already evicted or removed?"
            );
        }

        moveToHead(entry);
        // size unchanged — entry was already counted
    }

    /**
     * Unlinks an entry from the DLL.
     *
     * Called in two contexts:
     *
     * 1. CachePartition.delete() — explicit deletion by caller.
     *
     * 2. CachePartition.get() — lazy TTL expiration.
     *    When get() reads an entry and finds it expired, it removes it
     *    from both the map and the DLL. onRemove handles the DLL side.
     *
     * Idempotent: if entry.prev == null (already unlinked), returns
     * silently. This handles the case where TTL reaper and lazy expiration
     * both try to remove the same entry — the second call is a no-op.
     *
     * CALLER MUST HOLD: partition write lock.
     */
    @Override
    public void onRemove(CacheEntry<K, V> entry) {
        validateRealEntry(entry);

        if (entry.prev == null) {
            // Already unlinked — idempotent, not an error.
            // Scenario: TTL reaper removed the entry; then CachePartition.get()
            // also tries to remove it via lazy expiration. Second call is safe.
            return;
        }

        unlinkNode(entry);
        size--;
    }

    /**
     * Evicts the Least Recently Used entry (dummyTail.prev).
     *
     * Called by CachePartition.put() when the partition is at capacity
     * and a new entry must be inserted.
     *
     * Returns the evicted entry so CachePartition can:
     *   1. Remove it from the primary map (ConcurrentHashMap)
     *   2. Optionally: log it, emit a metric, notify a listener
     *
     * This method handles ONLY the DLL side. It does not touch the map.
     * Single responsibility — eviction strategy manages ordering,
     * partition manages the primary index.
     *
     * Returns null if the list is empty (size == 0).
     * CachePartition must handle the null case — it means the partition
     * has no entries to evict (should not happen if capacity logic is correct,
     * but defensive null return is safer than throwing).
     *
     * CALLER MUST HOLD: partition write lock.
     */
    @Override
    public CacheEntry<K, V> evict() {
        if (size == 0) return null;

        CacheEntry<K, V> lru = dummyTail.prev;

        // Sanity check: if dummyTail.prev IS dummyHead, the list is
        // logically empty despite size > 0 — size accounting bug.
        // Throw rather than evict a sentinel node.
        if (lru == dummyHead) {
            throw new IllegalStateException(
                    "evict() found empty DLL but size=" + size +
                            ". Size accounting is incorrect — check onInsert/onRemove pairing."
            );
        }

        unlinkNode(lru);
        size--;
        return lru;
    }

    /**
     * Returns the number of real entries currently tracked.
     * O(1) — maintained as an explicit field, not computed by traversal.
     *
     * CALLER MUST HOLD: partition read or write lock.
     * (size is not volatile — visibility provided by lock)
     */
    @Override
    public int size() {
        return size;
    }

    // ─────────────────────────────────────────────────────────────
    // Private DLL primitives
    // All three require caller to hold write lock (same as public methods)
    // ─────────────────────────────────────────────────────────────

    /**
     * Inserts entry immediately after dummyHead (MRU position).
     *
     * Pointer operations (4 writes, exact order matters):
     *
     * Before:
     *   dummyHead <──► FIRST <──► ...
     *
     * After:
     *   dummyHead <──► entry <──► FIRST <──► ...
     *
     * Step 1: entry.prev = dummyHead
     *   entry now knows its left neighbor
     *
     * Step 2: entry.next = dummyHead.next
     *   entry now knows its right neighbor (FIRST)
     *   MUST happen before step 4 — after step 4, dummyHead.next is entry,
     *   so we'd lose the reference to FIRST
     *
     * Step 3: dummyHead.next.prev = entry
     *   FIRST now knows its new left neighbor (entry)
     *   MUST happen before step 4 — same reason as step 2
     *
     * Step 4: dummyHead.next = entry
     *   dummyHead now knows its new right neighbor (entry)
     *   This is the last step — completes the connection
     */
    private void addToHead(CacheEntry<K, V> entry) {
        entry.prev = dummyHead;           // step 1
        entry.next = dummyHead.next;      // step 2
        dummyHead.next.prev = entry;      // step 3
        dummyHead.next = entry;           // step 4
    }

    /**
     * Removes entry from its current position in the DLL.
     *
     * Pointer operations (4 writes):
     *
     * Before:
     *   PREV <──► entry <──► NEXT
     *
     * After:
     *   PREV <──► NEXT
     *   entry.prev = null, entry.next = null
     *
     * Step 1: entry.prev.next = entry.next
     *   PREV now skips over entry, points directly to NEXT
     *
     * Step 2: entry.next.prev = entry.prev
     *   NEXT now skips back over entry, points directly to PREV
     *   Steps 1+2: PREV <──► NEXT (entry fully bypassed)
     *
     * Step 3: entry.prev = null
     * Step 4: entry.next = null
     *   Null signals "not linked" — detected by onAccess, onRemove.
     *   Also breaks reference chain for GC — evicted entry no longer
     *   holds references to its former neighbors.
     *
     * Order of steps 3 and 4:
     *   Could be either order — steps 1 and 2 already bypassed entry,
     *   so nulling prev/next does not affect PREV or NEXT.
     */
    private void unlinkNode(CacheEntry<K, V> entry) {
        entry.prev.next = entry.next;     // step 1
        entry.next.prev = entry.prev;     // step 2
        entry.prev = null;                // step 3
        entry.next = null;                // step 4
    }

    /**
     * Moves an already-linked entry to MRU position.
     *
     * Composition of the two primitives:
     *   unlinkNode: detach from current position (4 ptr writes)
     *   addToHead:  attach at MRU position (4 ptr writes)
     *   Total: 8 pointer writes per cache read
     *
     * Size is unchanged — entry was already counted in size.
     * unlinkNode does NOT decrement here — size change only happens
     * in onRemove() and evict() where the entry leaves permanently.
     * To prevent double-decrement, unlinkNode itself does not touch size —
     * size management is the responsibility of the calling public method.
     */
    private void moveToHead(CacheEntry<K, V> entry) {
        unlinkNode(entry);
        addToHead(entry);
    }

    /**
     * Guards against null or sentinel entries being passed to public methods.
     *
     * Sentinels have null keys (from the protected no-arg constructor).
     * A null key means it's either a literal null or a sentinel —
     * both are invalid arguments for public methods.
     *
     * Why check for null key rather than identity (entry == dummyHead)?
     *   dummyHead and dummyTail are private — no external code can hold
     *   a reference to them. But a null CacheEntry or a CacheEntry with
     *   null key (e.g., someone subclassed incorrectly) must be caught.
     *   Checking entry.key == null catches both cases safely.
     */
    private void validateRealEntry(CacheEntry<K, V> entry) {
        if (entry == null || entry.key == null) {
            throw new IllegalArgumentException(
                    "Cannot operate on null or sentinel entries. " +
                            "Only real CacheEntry instances with non-null keys are valid."
            );
        }
    }
}