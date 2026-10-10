package org.cache.core;
/*
Strategy pattern interface
 */
public interface EvictionStrategy<K,V> {
    /*
    Triggered whenever an existing key is read or updated
    Repositions the node within the eviction  data structure.
     */

    void onAccess(CacheEntry<K,V> entry);

    // new entry is added.
    void onInsert(CacheEntry<K,V> entry);

    // Triggered when an entry is deleted manually or lazy-expiration.

    void onRemove(CacheEntry<K,V> entry);

    /*
    Evaluates and ejects the ideal victim node according to the strategy rules.
    @return the evicted entry that be removed from the primary index.
     */
    CacheEntry<K,V> evict();

    // return the current number of tracked nodes.

    int size();



}
