package org.cache.core;

/**
 * Factory interface for creating CacheEntry instances.
 *
 * WHY THIS EXISTS:
 * CachePartition needs to create new entries on every put().
 * But CachePartition does not know which entry type to create:
 *   LRU → needs plain CacheEntry
 *   LFU → needs LFUCacheEntry (has frequency field)
 *
 * This interface hides that decision from CachePartition.
 * CachePartition calls create() — factory returns the right type.
 *
 * This is the Factory Method pattern:
 *   The interface defines WHAT to create (a CacheEntry).
 *   The implementation decides HOW to create it (which subtype).
 */
public interface CacheEntryFactory<K, V> {

    /**
     * Creates a new CacheEntry of the appropriate type.
     *
     * @param key    the key — must be non-null
     * @param value  the value — may be null (negative caching)
     * @param ttlMs  time to live in milliseconds, 0 = no expiry
     * @return a new CacheEntry ready to be inserted into the partition
     */
    CacheEntry<K, V> create(K key, V value, long ttlMs);
}