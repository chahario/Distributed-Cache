package org.cache.core;

/**
 * Creates LFUCacheEntry instances.
 * Use this with LFUEvictionStrategy.
 *
 * LFUCacheEntry adds to CacheEntry:
 *   frequency (double) — decaying access counter
 *   lastAccessedAt (long) — epoch ms of last access
 * Memory: 64 bytes per entry shell (+16 bytes vs plain CacheEntry).
 *
 * WHY REQUIRED FOR LFU:
 * LFUEvictionStrategy.castToLFU() checks instanceof LFUCacheEntry.
 * If you use DefaultEntryFactory with LFUEvictionStrategy,
 * castToLFU() throws IllegalArgumentException immediately on first insert.
 * This factory ensures the right type is always created.
 */
public class LFUEntryFactory<K, V> implements CacheEntryFactory<K, V> {

    @Override
    public CacheEntry<K, V> create(K key, V value, long ttlMs) {
        return new LFUCacheEntry<>(key, value, ttlMs);
    }
}