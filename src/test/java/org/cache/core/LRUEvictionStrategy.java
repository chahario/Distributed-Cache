package org.cache.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LRUEvictionStrategy — the doubly-linked list mechanics.
 *
 * WHY THIS TEST CLASS IS IN package org.cache.core:
 * ─────────────────────────────────────────────────────────────────
 * CacheEntry.prev, CacheEntry.next, and CacheEntry.key are all
 * package-private. Declaring this test in the same package lets us
 * assert directly on the DLL pointers instead of adding public
 * getters purely for testing.
 *
 * Java package-private access is per PACKAGE NAME, not per folder —
 * src/main/java/org/cache/core and src/test/java/org/cache/core are
 * the same package as far as the compiler is concerned.
 *
 * WHAT THIS CLASS DOES NOT TEST:
 * ─────────────────────────────────────────────────────────────────
 * Thread safety. LRUEvictionStrategy is deliberately NOT thread-safe —
 * CachePartition's write lock is what protects it. Concurrency is
 * covered in CachePartitionTest and ConcurrencyStressTest.
 */
class LRUEvictionStrategyTest {

    private LRUEvictionStrategy<String, String> strategy;
    private DefaultEntryFactory<String, String> factory;

    @BeforeEach
    void setUp() {
        strategy = new LRUEvictionStrategy<>();
        factory  = new DefaultEntryFactory<>();
    }

    /** Convenience: build a real entry for the given key. */
    private CacheEntry<String, String> entry(String key) {
        return factory.create(key, "value-" + key, 0);
    }

    // ═════════════════════════════════════════════════════════════
    // Insertion
    // ═════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("onInsert")
    class OnInsert {

        @Test
        @DisplayName("first insert becomes both MRU and LRU")
        void firstInsertIsBothEnds() {
            CacheEntry<String, String> a = entry("a");
            strategy.onInsert(a);

            assertEquals(1, strategy.size());

            // With one entry, it is simultaneously the most and least
            // recently used. evict() must return it.
            assertEquals("a", strategy.evict().getKey());
        }

        @Test
        @DisplayName("inserts stack at the head — newest first")
        void insertsStackAtHead() {
            strategy.onInsert(entry("a"));
            strategy.onInsert(entry("b"));
            strategy.onInsert(entry("c"));

            assertEquals(3, strategy.size());

            // c was inserted last → it sits at MRU
            // a was inserted first → it sits at LRU → evicted first
            assertEquals("a", strategy.evict().getKey());
            assertEquals("b", strategy.evict().getKey());
            assertEquals("c", strategy.evict().getKey());
        }

        @Test
        @DisplayName("onInsert links prev and next — prev != null after insert")
        void insertSetsPointers() {
            CacheEntry<String, String> a = entry("a");

            // Before insert: unlinked. This null is the signal the
            // strategy uses to detect "not in any eviction list".
            assertNull(a.prev, "a fresh entry must start unlinked");
            assertNull(a.next);

            strategy.onInsert(a);

            assertNotNull(a.prev, "after insert, prev points to dummyHead");
            assertNotNull(a.next, "after insert, next points to dummyTail");
        }

        @Test
        @DisplayName("inserting an already-linked entry throws")
        void doubleInsertThrows() {
            CacheEntry<String, String> a = entry("a");
            strategy.onInsert(a);

            IllegalStateException ex = assertThrows(
                    IllegalStateException.class,
                    () -> strategy.onInsert(a)
            );

            assertTrue(ex.getMessage().contains("already-linked"),
                    "The message must tell the caller what they did wrong. " +
                            "Silently re-inserting would corrupt the DLL: the entry " +
                            "would appear twice, size would over-count, and evict() " +
                            "would eventually return a node that is not in the list.");
        }

        @Test
        @DisplayName("null entry is rejected")
        void nullEntryRejected() {
            assertThrows(IllegalArgumentException.class,
                    () -> strategy.onInsert(null));
        }
    }

    // ═════════════════════════════════════════════════════════════
    // Access — the moveToHead path, runs on every cache read
    // ═════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("onAccess")
    class OnAccess {

        @Test
        @DisplayName("accessing the LRU entry saves it from eviction")
        void accessPromotesToMru() {
            CacheEntry<String, String> a = entry("a");
            CacheEntry<String, String> b = entry("b");
            CacheEntry<String, String> c = entry("c");

            strategy.onInsert(a);   // [a]
            strategy.onInsert(b);   // [b, a]
            strategy.onInsert(c);   // [c, b, a]  — a is LRU

            strategy.onAccess(a);   // [a, c, b]  — b is now LRU

            assertEquals("b", strategy.evict().getKey(),
                    "This is the entire point of LRU: touching 'a' moved it " +
                            "out of the eviction position, and 'b' inherited it.");
        }

        @Test
        @DisplayName("access does not change size")
        void accessDoesNotChangeSize() {
            CacheEntry<String, String> a = entry("a");
            strategy.onInsert(a);
            assertEquals(1, strategy.size());

            for (int i = 0; i < 100; i++) {
                strategy.onAccess(a);
            }

            assertEquals(1, strategy.size(),
                    "moveToHead is unlink + addToHead. If unlinkNode " +
                            "decremented size and addToHead incremented it, this " +
                            "would still be 1 — but any asymmetry shows up here.");
        }

        @Test
        @DisplayName("accessing the only entry is a no-op structurally")
        void accessSingleEntry() {
            CacheEntry<String, String> a = entry("a");
            strategy.onInsert(a);

            // Unlink then relink at the same position. Easy place for
            // an off-by-one pointer bug to hide.
            assertDoesNotThrow(() -> strategy.onAccess(a));
            assertEquals(1, strategy.size());
            assertEquals("a", strategy.evict().getKey());
        }

        @Test
        @DisplayName("accessing the MRU entry keeps it at MRU")
        void accessMruStaysMru() {
            CacheEntry<String, String> a = entry("a");
            CacheEntry<String, String> b = entry("b");

            strategy.onInsert(a);   // [a]
            strategy.onInsert(b);   // [b, a] — b is MRU

            strategy.onAccess(b);   // [b, a] — unchanged

            assertEquals("a", strategy.evict().getKey());
            assertEquals("b", strategy.evict().getKey());
        }

        @Test
        @DisplayName("repeated access keeps the list consistent")
        void repeatedAccessStaysConsistent() {
            CacheEntry<String, String> a = entry("a");
            CacheEntry<String, String> b = entry("b");
            CacheEntry<String, String> c = entry("c");

            strategy.onInsert(a);
            strategy.onInsert(b);
            strategy.onInsert(c);

            // Hammer the pointer rewiring — 3000 moveToHead calls.
            // Any pointer leak shows up as a broken list at the end.
            for (int i = 0; i < 1000; i++) {
                strategy.onAccess(a);
                strategy.onAccess(b);
                strategy.onAccess(c);
            }

            assertEquals(3, strategy.size());

            // Last access order was a, b, c → c is MRU, a is LRU
            assertEquals("a", strategy.evict().getKey());
            assertEquals("b", strategy.evict().getKey());
            assertEquals("c", strategy.evict().getKey());
        }

        @Test
        @DisplayName("accessing an unlinked entry throws")
        void accessUnlinkedThrows() {
            CacheEntry<String, String> a = entry("a");
            // Never inserted

            IllegalStateException ex = assertThrows(
                    IllegalStateException.class,
                    () -> strategy.onAccess(a)
            );

            assertTrue(ex.getMessage().contains("unlinked"),
                    "Silently proceeding would call unlinkNode on an entry " +
                            "whose prev is null → NullPointerException deep inside " +
                            "the DLL, far from the actual mistake.");
        }

        @Test
        @DisplayName("accessing an already-evicted entry throws")
        void accessEvictedThrows() {
            CacheEntry<String, String> a = entry("a");
            strategy.onInsert(a);
            strategy.evict();   // now unlinked

            assertThrows(IllegalStateException.class,
                    () -> strategy.onAccess(a));
        }
    }

    // ═════════════════════════════════════════════════════════════
    // Removal
    // ═════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("onRemove")
    class OnRemove {

        @Test
        @DisplayName("removing from the middle relinks the neighbours")
        void removeFromMiddle() {
            CacheEntry<String, String> a = entry("a");
            CacheEntry<String, String> b = entry("b");
            CacheEntry<String, String> c = entry("c");

            strategy.onInsert(a);
            strategy.onInsert(b);
            strategy.onInsert(c);   // [c, b, a]

            strategy.onRemove(b);   // [c, a]

            assertEquals(2, strategy.size());

            // If unlinkNode failed to bridge c and a, the list would
            // be broken and this would return the wrong key or throw.
            assertEquals("a", strategy.evict().getKey());
            assertEquals("c", strategy.evict().getKey());
        }

        @Test
        @DisplayName("removing the head works")
        void removeHead() {
            CacheEntry<String, String> a = entry("a");
            CacheEntry<String, String> b = entry("b");

            strategy.onInsert(a);
            strategy.onInsert(b);   // [b, a] — b is head

            strategy.onRemove(b);

            assertEquals(1, strategy.size());
            assertEquals("a", strategy.evict().getKey());
        }

        @Test
        @DisplayName("removing the tail works")
        void removeTail() {
            CacheEntry<String, String> a = entry("a");
            CacheEntry<String, String> b = entry("b");

            strategy.onInsert(a);   // a is tail
            strategy.onInsert(b);

            strategy.onRemove(a);

            assertEquals(1, strategy.size());
            assertEquals("b", strategy.evict().getKey());
        }

        @Test
        @DisplayName("onRemove nulls the entry's pointers")
        void removeNullsPointers() {
            CacheEntry<String, String> a = entry("a");
            strategy.onInsert(a);
            assertNotNull(a.prev);

            strategy.onRemove(a);

            assertNull(a.prev,
                    "prev == null is the 'unlinked' signal. It also breaks " +
                            "the reference chain so GC can reclaim neighbours.");
            assertNull(a.next);
        }

        @Test
        @DisplayName("double remove is a silent no-op, not an exception")
        void doubleRemoveIsIdempotent() {
            CacheEntry<String, String> a = entry("a");
            strategy.onInsert(a);

            strategy.onRemove(a);
            assertEquals(0, strategy.size());

            // This happens for real: the TTL reaper removes an expired
            // entry, then a concurrent get() also detects expiry and
            // tries to remove it. The second call must be harmless.
            assertDoesNotThrow(() -> strategy.onRemove(a));

            assertEquals(0, strategy.size(),
                    "Idempotent means size must NOT go negative on the " +
                            "second call.");
        }

        @Test
        @DisplayName("removing a never-inserted entry is safe")
        void removeNeverInsertedIsSafe() {
            CacheEntry<String, String> a = entry("a");
            assertDoesNotThrow(() -> strategy.onRemove(a));
            assertEquals(0, strategy.size());
        }
    }

    // ═════════════════════════════════════════════════════════════
    // Eviction
    // ═════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("evict")
    class Evict {

        @Test
        @DisplayName("evicts in strict insertion order when nothing is accessed")
        void evictsInInsertionOrder() {
            for (int i = 0; i < 10; i++) {
                strategy.onInsert(entry("k" + i));
            }

            for (int i = 0; i < 10; i++) {
                assertEquals("k" + i, strategy.evict().getKey(),
                        "With no accesses, LRU order == insertion order (FIFO)");
            }

            assertEquals(0, strategy.size());
        }

        @Test
        @DisplayName("empty strategy returns null, not an exception")
        void evictEmptyReturnsNull() {
            assertNull(strategy.evict(),
                    "CachePartition calls evict() when at capacity. A null " +
                            "return is easier to handle defensively than an exception " +
                            "for a condition that shouldn't happen but might.");
        }

        @Test
        @DisplayName("evicted entry is fully unlinked")
        void evictedEntryIsUnlinked() {
            CacheEntry<String, String> a = entry("a");
            strategy.onInsert(a);

            CacheEntry<String, String> victim = strategy.evict();

            assertSame(a, victim, "evict returns the actual entry object " +
                    "so CachePartition can call map.remove(victim.getKey())");
            assertNull(victim.prev);
            assertNull(victim.next);
        }

        @Test
        @DisplayName("evict decrements size exactly once")
        void evictDecrementsSize() {
            strategy.onInsert(entry("a"));
            strategy.onInsert(entry("b"));
            assertEquals(2, strategy.size());

            strategy.evict();
            assertEquals(1, strategy.size());

            strategy.evict();
            assertEquals(0, strategy.size());

            strategy.evict();   // no-op on empty
            assertEquals(0, strategy.size(),
                    "size must not go negative when evicting an empty list");
        }
    }

    // ═════════════════════════════════════════════════════════════
    // Sentinel isolation
    // ═════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("sentinel nodes")
    class Sentinels {

        @Test
        @DisplayName("sentinels are never returned by evict")
        void sentinelsNeverEvicted() {
            // Cycle the list many times. If a sentinel ever leaks out
            // of evict(), getKey() returns null and this blows up.
            for (int round = 0; round < 100; round++) {
                strategy.onInsert(entry("k" + round));
                CacheEntry<String, String> victim = strategy.evict();

                assertNotNull(victim);
                assertNotNull(victim.getKey(),
                        "A sentinel has a null key. If one escapes here, " +
                                "CachePartition would call map.remove(null).");
                assertEquals("k" + round, victim.getKey());
            }
        }

        @Test
        @DisplayName("size never counts the two sentinels")
        void sizeExcludesSentinels() {
            assertEquals(0, strategy.size(),
                    "Fresh strategy holds dummyHead and dummyTail but " +
                            "reports size 0 — sentinels are structure, not data");
        }
    }

    // ═════════════════════════════════════════════════════════════
    // Mixed sequences — where pointer bugs actually surface
    // ═════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("mixed operation sequences")
    class MixedSequences {

        @Test
        @DisplayName("insert / access / remove interleaved stays consistent")
        void interleavedOperations() {
            CacheEntry<String, String> a = entry("a");
            CacheEntry<String, String> b = entry("b");
            CacheEntry<String, String> c = entry("c");
            CacheEntry<String, String> d = entry("d");

            strategy.onInsert(a);       // [a]
            strategy.onInsert(b);       // [b, a]
            strategy.onAccess(a);       // [a, b]
            strategy.onInsert(c);       // [c, a, b]
            strategy.onRemove(a);       // [c, b]
            strategy.onInsert(d);       // [d, c, b]
            strategy.onAccess(b);       // [b, d, c]

            assertEquals(3, strategy.size());

            assertEquals("c", strategy.evict().getKey());
            assertEquals("d", strategy.evict().getKey());
            assertEquals("b", strategy.evict().getKey());
            assertNull(strategy.evict());
        }

        @Test
        @DisplayName("re-inserting a previously evicted entry works")
        void reinsertAfterEvict() {
            CacheEntry<String, String> a = entry("a");

            strategy.onInsert(a);
            strategy.evict();               // a is unlinked, prev == null

            // The Object Pool pattern would do exactly this.
            // onInsert must accept it because prev is null again.
            assertDoesNotThrow(() -> strategy.onInsert(a));
            assertEquals(1, strategy.size());
            assertEquals("a", strategy.evict().getKey());
        }

        @Test
        @DisplayName("1000 entries with random access stays structurally sound")
        void largeSequenceStaysConsistent() {
            int n = 1000;
            CacheEntry<String, String>[] entries = new CacheEntry[n];

            for (int i = 0; i < n; i++) {
                entries[i] = entry("k" + i);
                strategy.onInsert(entries[i]);
            }
            assertEquals(n, strategy.size());

            // 5000 random accesses — heavy pointer churn
            java.util.Random rng = new java.util.Random(42);   // fixed seed
            for (int i = 0; i < 5000; i++) {
                strategy.onAccess(entries[rng.nextInt(n)]);
            }

            assertEquals(n, strategy.size(),
                    "Accesses must never change size");

            // Drain the whole list. If any pointer is corrupt, this
            // either throws, returns null early, or returns a duplicate.
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (int i = 0; i < n; i++) {
                CacheEntry<String, String> victim = strategy.evict();
                assertNotNull(victim, "list emptied early at i=" + i);
                assertTrue(seen.add(victim.getKey()),
                        "duplicate eviction: " + victim.getKey() +
                                " — the DLL has a cycle");
            }

            assertNull(strategy.evict());
            assertEquals(0, strategy.size());
            assertEquals(n, seen.size(), "every entry evicted exactly once");
        }
    }
}