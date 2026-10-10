package org.cache.hash;

import java.nio.charset.StandardCharsets;

/**
 * MurmurHash3 — fast non-cryptographic hash function.
 *
 * WHY NOT String.hashCode()?
 * ─────────────────────────────────────────────────────────────────
 * String.hashCode() is a polynomial hash:
 *   hash = 31 * hash + char
 *
 * Problems:
 *   1. Poor avalanche property:
 *      "user:1" and "user:2" differ by 1 in hash value.
 *      For 32 partitions this is OK.
 *      For consistent hash ring (millions of positions) — terrible.
 *      Consecutive hashes cluster together on the ring.
 *      Some servers get 60% of keys, others get 10%.
 *
 *   2. Not designed for distribution:
 *      hashCode() designed for HashMap bucket lookup, not ring routing.
 *
 * MurmurHash3 properties:
 *   Avalanche:       tiny input change → completely different output
 *   Uniform:         keys spread evenly across full hash space
 *   Fast:            2-4 GB/s — faster than cryptographic hashes
 *   Deterministic:   same input → same output across all JVMs
 *   Non-cryptographic: not for security, perfect for routing
 *
 * Used by: Redis Cluster, Cassandra, Guava, Kafka
 *
 * WHERE WE USE IT:
 *   ConsistentHashRing → routes a key to a server on the ring, and
 *   places each node's virtual nodes. This is the one place that
 *   needs strong avalanche and cross-JVM determinism, so every node
 *   computes an identical ring (see ConsistentHashRing).
 *
 * NOTE: partition striping inside a single node (StripedCache) does
 * NOT use this — it routes with the far cheaper
 * (key.hashCode() & 0x7FFFFFFF) % NUM_PARTITIONS, which is good enough
 * for spreading keys across 32 local locks. hash32() below is provided
 * for callers that want a better-distributed 32-bit hash, but is not
 * currently on the partition hot path.
 */
public class MurmurHash3 {

    /**
     * Seed value for the hash function.
     * Arbitrary constant — same seed always produces same output.
     * Different seeds produce different (but equally valid) outputs.
     * Must be the same across all nodes in a cluster.
     */
    private static final int SEED = 42;

    // Private constructor — all methods are static, no instances needed
    private MurmurHash3() {}

    /**
     * Hashes a String key to a positive 32-bit integer.
     *
     * Always returns positive value:
     *   Math.abs() handles the negative case.
     *   Safe to use directly with % NUM_PARTITIONS.
     *   No need for & 0x7FFFFFFF trick (Math.abs handles it).
     *
     * Note: Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE (still negative!)
     * We handle this edge case below.
     *
     * @param key the string to hash — must not be null
     * @return positive 32-bit hash value
     */
    public static int hash32(String key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");

        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        int hash = murmur3_32(data, SEED);

        // Math.abs(Integer.MIN_VALUE) is still negative — handle edge case
        return hash == Integer.MIN_VALUE ? 0 : Math.abs(hash);
    }

    /**
     * Core MurmurHash3 32-bit implementation.
     * Reference: Austin Appleby's public domain MurmurHash3.
     *
     * Algorithm:
     *   1. Process input in 4-byte blocks
     *   2. Process remaining bytes (tail)
     *   3. Finalization mix (fmix) — ensures avalanche property
     *
     * @param data  input bytes
     * @param seed  starting seed
     * @return 32-bit hash (may be negative)
     */
    private static int murmur3_32(byte[] data, int seed) {
        // Two mixing constants from Austin Appleby's reference implementation
        // These specific values chosen for optimal avalanche properties
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1     = seed;
        int length = data.length;
        int blocks = length / 4;  // how many complete 4-byte blocks

        // ── Phase 1: Process complete 4-byte blocks ──────────────
        for (int i = 0; i < blocks; i++) {
            int k1 = getInt(data, i * 4);

            k1 *= c1;
            k1  = Integer.rotateLeft(k1, 15);  // rotate bits left by 15
            k1 *= c2;

            h1 ^= k1;
            h1  = Integer.rotateLeft(h1, 13);  // rotate bits left by 13
            h1  = h1 * 5 + 0xe6546b64;
        }

        // ── Phase 2: Process remaining 1-3 bytes (tail) ──────────
        // If string length is not a multiple of 4, handle leftover bytes
        int tail = blocks * 4;
        int k1   = 0;

        switch (length - tail) {
            case 3:
                k1 ^= (data[tail + 2] & 0xff) << 16;
                // fall through intentional
            case 2:
                k1 ^= (data[tail + 1] & 0xff) << 8;
                // fall through intentional
            case 1:
                k1 ^= (data[tail] & 0xff);
                k1 *= c1;
                k1  = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }

        // ── Phase 3: Finalization ─────────────────────────────────
        h1 ^= length;      // XOR with length — different length = different hash
        h1  = fmix32(h1);  // avalanche mix — every bit affects every other bit

        return h1;
    }

    /**
     * Finalization mix — the avalanche function.
     *
     * Purpose: ensure every input bit affects every output bit.
     * Without this, small input changes would cause small output changes.
     * With this: changing ONE input bit changes ~50% of output bits on average.
     *
     * This is called the "avalanche property" — critical for uniform distribution.
     */
    private static int fmix32(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    /**
     * Reads 4 bytes from data starting at index as a little-endian int.
     *
     * Little-endian: least significant byte first.
     * data[0] = lowest bits, data[3] = highest bits.
     * This matches x86 CPU byte order — no conversion needed on most hardware.
     *
     * & 0xff: converts signed byte (-128 to 127) to unsigned int (0 to 255).
     * Without this: a byte value of -1 (0xFF) would sign-extend to 0xFFFFFFFF
     * corrupting the hash.
     */
    private static int getInt(byte[] data, int index) {
        return  (data[index]     & 0xff)
                | ((data[index + 1]  & 0xff) << 8)
                | ((data[index + 2]  & 0xff) << 16)
                | ((data[index + 3]  & 0xff) << 24);
    }

    /**
     * Hashes a String to a 64-bit long.
     *
     * WHY 64-BIT FOR THE RING (when 32-bit works for partitions)?
     * ─────────────────────────────────────────────────────────────
     * Partition routing: 32 slots. A 32-bit hash gives 4.3 billion
     * possible values mapped onto 32 partitions — plenty.
     *
     * Consistent hash ring: positions are the ring itself.
     * With 32-bit (4.3 billion positions) and 600 vnodes,
     * collisions are unlikely but possible — two vnodes landing
     * on the same position means one silently overwrites the other
     * in the TreeMap, and that server loses an arc.
     *
     * With 64-bit (18 quintillion positions), collision probability
     * is effectively zero. Cassandra and Redis Cluster both use
     * 64-bit or wider for exactly this reason.
     *
     * This is MurmurHash3's 128-bit variant, returning the first
     * 64 bits. Reference: Austin Appleby's public domain code.
     *
     * @param key the string to hash — must not be null
     * @return 64-bit hash (may be negative — the ring handles
     *         the full signed long range)
     */
    public static long hash64(String key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");

        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        return murmur3_64(data, SEED);
    }

    /**
     * 64-bit MurmurHash3 core.
     *
     * Processes input in 8-byte blocks (vs 4-byte for 32-bit),
     * then handles the tail, then applies the finalization mix.
     */
    private static long murmur3_64(byte[] data, long seed) {
        final long c1 = 0x87c37b91114253d5L;
        final long c2 = 0x4cf5ad432745937fL;

        long h1     = seed;
        int  length = data.length;
        int  blocks = length / 8;

        // ── Process complete 8-byte blocks ────────────────────────
        for (int i = 0; i < blocks; i++) {
            long k1 = getLong(data, i * 8);

            k1 *= c1;
            k1  = Long.rotateLeft(k1, 31);
            k1 *= c2;

            h1 ^= k1;
            h1  = Long.rotateLeft(h1, 27);
            h1  = h1 * 5 + 0x52dce729;
        }

        // ── Process remaining 1-7 bytes ───────────────────────────
        long k1   = 0;
        int  tail = blocks * 8;

        switch (length - tail) {
            case 7: k1 ^= ((long) data[tail + 6] & 0xff) << 48;
            case 6: k1 ^= ((long) data[tail + 5] & 0xff) << 40;
            case 5: k1 ^= ((long) data[tail + 4] & 0xff) << 32;
            case 4: k1 ^= ((long) data[tail + 3] & 0xff) << 24;
            case 3: k1 ^= ((long) data[tail + 2] & 0xff) << 16;
            case 2: k1 ^= ((long) data[tail + 1] & 0xff) << 8;
            case 1: k1 ^= ((long) data[tail] & 0xff);
                k1 *= c1;
                k1  = Long.rotateLeft(k1, 31);
                k1 *= c2;
                h1 ^= k1;
        }

        // ── Finalization mix — forces avalanche ───────────────────
        h1 ^= length;
        h1 ^= h1 >>> 33;
        h1 *= 0xff51afd7ed558ccdL;
        h1 ^= h1 >>> 33;
        h1 *= 0xc4ceb9fe1a85ec53L;
        h1 ^= h1 >>> 33;

        return h1;
    }

    /**
     * Reads 8 bytes as a little-endian long.
     * & 0xff converts each signed byte to unsigned before shifting —
     * without it, a byte of -1 sign-extends and corrupts the result.
     */
    private static long getLong(byte[] data, int index) {
        return  ((long) data[index]     & 0xff)
                | (((long) data[index + 1] & 0xff) << 8)
                | (((long) data[index + 2] & 0xff) << 16)
                | (((long) data[index + 3] & 0xff) << 24)
                | (((long) data[index + 4] & 0xff) << 32)
                | (((long) data[index + 5] & 0xff) << 40)
                | (((long) data[index + 6] & 0xff) << 48)
                | (((long) data[index + 7] & 0xff) << 56);
    }
}