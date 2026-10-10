package org.cache.core;

/**
 * Creates plain CacheEntry instances.
 * Use this with LRUEvictionStrategy.
 *
 * Plain CacheEntry has:
 *   key, value, ttlMs, createdAt, prev, next
 * No frequency tracking — LRU does not need it.
 * Memory: 48 bytes per entry shell.
 */
public class DefaultEntryFactory<K, V> implements CacheEntryFactory<K, V> {

    @Override
    public CacheEntry<K, V> create(K key, V value, long ttlMs) {
        return new CacheEntry<>(key, value, ttlMs);
    }
}