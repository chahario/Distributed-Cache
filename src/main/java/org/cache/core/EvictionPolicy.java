package org.cache.core;

/**
 * Selects the eviction algorithm for StripedCache.
 *
 * LRU: evicts the least recently ACCESSED entry.
 *      Best for: workloads where recent access predicts future access.
 *      Default choice for most caches.
 *
 * LFU: evicts the least frequently accessed entry (with decay).
 *      Best for: workloads with a stable hot set accessed repeatedly.
 *      Worse for: scan workloads, new entry heavy workloads.
 */
public enum EvictionPolicy {
    LRU,
    LFU
}