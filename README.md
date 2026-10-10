# Distributed In-Memory Cache

A distributed, sharded, in-memory key–value cache built from scratch in Java — a minimal Redis/Memcached. It shards data across nodes with **consistent hashing**, stays correct under heavy concurrency via **lock striping**, supports **LRU/LFU** eviction and **TTL** expiry, and speaks a compact **12-byte binary protocol** over a non-blocking **NIO** server.

> "A single cache box caps out on memory, throughput, and availability. I built a cache that spreads keys across nodes with consistent hashing, runs thousands of concurrent requests per node through 32 lock-striped partitions, and talks a compact binary protocol over NIO — trading features like replication for throughput and provable correctness."

---

## Table of Contents

1. [Problem & Solution](#problem--solution)
2. [Feature Highlights](#feature-highlights)
3. [Architecture](#architecture)
4. [Request Lifecycle](#request-lifecycle)
5. [Two-Level Hashing (how a key finds its home)](#two-level-hashing)
6. [Core Data Structures](#core-data-structures)
7. [Consistent Hashing](#consistent-hashing)
8. [Concurrency Model](#concurrency-model)
9. [Expiry / TTL](#expiry--ttl)
10. [The Wire Protocol](#the-wire-protocol)
11. [Cluster Mode & Transparent Forwarding](#cluster-mode--transparent-forwarding)
12. [Project Layout](#project-layout)
13. [Build & Run](#build--run)
14. [Benchmarks & Results](#benchmarks--results)
15. [Testing Strategy](#testing-strategy)
16. [Design Decisions & Tradeoffs (FAQ)](#design-decisions--tradeoffs-faq)
17. [Known Limitations & Roadmap](#known-limitations--roadmap)

---

## Problem & Solution

**Problem.** A single cache server is a bottleneck: it has a fixed memory ceiling, its throughput is limited by one machine, and it's a single point of failure. Under concurrent access, a naive cache with one global lock serializes every request.

**Solution.** A cache that:
- **Scales horizontally** — data is partitioned across many nodes using a consistent-hash ring, so any node can route any key to its owner.
- **Scales vertically** — inside each node, keys are striped across 32 independently-locked partitions, so thousands of requests run in parallel.
- **Manages memory** — LRU or LFU eviction when full, plus TTL expiry (lazy + background sweep).
- **Is fast on the wire** — a fixed 12-byte binary header over a single-threaded NIO event loop, instead of HTTP.

The design deliberately optimizes for **throughput and simple, provable correctness** over features like replication and dynamic membership, which are treated as the next layers (see [Roadmap](#known-limitations--roadmap)).

---

## Feature Highlights

- **Consistent hashing** with 200 virtual nodes/physical node (~3% load imbalance) using 64-bit MurmurHash3.
- **Lock striping** — 32 partitions, each with its own `ReentrantReadWriteLock`.
- **Pluggable eviction** (Strategy pattern): **LRU** (doubly-linked list + sentinels) and **LFU** (frequency buckets with decay).
- **TTL expiry** — lazy on read + a background `TTLReaper` sweep.
- **Non-blocking server** — one NIO selector thread handles all connections.
- **Custom binary protocol** — 12-byte request header with length-prefixed framing.
- **Transparent clustering** — a client can hit any node; requests for keys owned elsewhere are forwarded one hop and relayed back.
- **Graceful lifecycle** — dependency-ordered startup/shutdown with a JVM shutdown hook.

---

## Architecture

```
                        ┌─────────────────────────────────────────────┐
   CLIENT               │                 ONE CACHE NODE               │
 ┌──────────┐  TCP      │  ┌────────────┐                              │
 │CacheClient├─────────►│  │ NIOServer  │  front door (1 selector thr) │
 │(blocking)│  0xCAFE   │  └─────┬──────┘                              │
 └──────────┘  binary   │        │ per-connection ConnectionHandler    │
                        │        │ (state machine: frames byte stream) │
                        │        ▼                                     │
                        │  ┌────────────┐   who owns this key?         │
                        │  │ClusterRouter├───► ConsistentHashRing       │
                        │  └─────┬──────┘        (MurmurHash64 + TreeMap)│
                        │   local│  remote│                            │
                        │        │        └──► CacheClient ──TCP──► other node
                        │        ▼                                     │
                        │  ┌──────────────┐  32 striped partitions     │
                        │  │ StripedCache │                            │
                        │  └──────┬───────┘                            │
                        │   ┌─────┴────┬──────────┬─── ... (32) ───┐   │
                        │   │Partition0│Partition1│      ...        │   │
                        │   └──────────┴──────────┴────────────────┘   │
                        │   each = ConcurrentHashMap                   │
                        │        + EvictionStrategy (LRU/LFU)          │
                        │        + ReentrantReadWriteLock              │
                        │                                             │
                        │  ┌────────────┐  background daemon           │
                        │  │ TTLReaper  │  scans all 32 every 1s       │
                        │  └────────────┘                             │
                        └─────────────────────────────────────────────┘
```

`CacheNode` owns and wires the components together. **Single-node vs cluster mode is decided by one thing:** whether the config carries a `ClusterConfig`. If not, `NIOServer` talks to `StripedCache` directly (no ring, no forwarding overhead).

**Startup order** is dependency-driven: `StripedCache → TTLReaper → ClusterRouter → NIOServer → shutdown hook`. Shutdown is the exact reverse (stop accepting requests *first*). The `running` flag flips to `true` only after every component starts successfully.

---

## Request Lifecycle

`GET user:42` end-to-end:

1. `CacheClient` encodes a 12-byte binary request and writes it fully to the socket.
2. The `NIOServer` selector wakes; `ConnectionHandler` reassembles the byte stream into a complete message.
3. `processRequest` asks `ClusterRouter` who owns the key → the ring hashes it and walks clockwise.
4. **Local** → serve from `StripedCache`. **Remote** → forward one hop to the owner over a pooled connection and relay the reply.
5. On the owning node, `StripedCache` picks 1 of 32 partitions; the partition locks, checks TTL, updates eviction order, returns the value.
6. Response encoded as a 5-byte header + body and written back.

---

## Two-Level Hashing

The cache hashes each key **twice**, with two functions, for two different purposes. Keeping these distinct is essential:

| Level | Question | Hash function | Structure |
|-------|----------|---------------|-----------|
| **1. Cluster routing** | *Which node owns this key?* | `MurmurHash3.hash64` | `TreeMap<Long,String>` ring |
| **2. Partition striping** | *Which of the 32 locks on that node?* | `key.hashCode()` | array index |

Level 1 picks the **machine**; Level 2 picks the **lock** inside that machine.

---

## Core Data Structures

Each shard is really **two data structures kept in lockstep under one lock**: a hash map for O(1) lookup, and an ordering structure for eviction.

```
StripedCache
│   CachePartition[] partitions        ← plain array of 32
│
└── each CachePartition:
      ├── ConcurrentHashMap<K, CacheEntry>   (primary index — O(1) lookup)
      ├── EvictionStrategy  (LRU DLL / LFU buckets — decides the victim)
      ├── ReentrantReadWriteLock  (concurrency)
      └── int maxSize = totalCapacity / 32
```

### CacheEntry — the shared node (the key trick)
`CacheEntry` holds `key`, `value`, `ttlMs`, `createdAt`, **and `prev`/`next` pointers**. The object stored as the map's **value** is *also* a **node** in the linked list — the map and the DLL point at the same object.

```
ConcurrentHashMap                 Doubly-Linked List (LRU order)
 "A" ───────────────►  dummyHead ⇄ [B] ⇄ [A] ⇄ [C] ⇄ dummyTail
 "B" ───────────────►             (MRU)          (LRU)
 "C" ───────────────►      same CacheEntry objects are the list nodes
```

This is **why LRU is O(1)**: `map.get(key)` returns the entry, which *is* the list node, so it can be unlinked/moved in O(1). Without the sharing, removing from the middle of the list would be O(n). `LFUCacheEntry` subclasses it to add `frequency` + `lastAccessedAt`, so LRU entries don't pay for fields they never use.

### ConcurrentHashMap — the primary index
Stores `K → CacheEntry`. Chosen over `HashMap` because the reaper iterates the map *while removing* expired entries — `HashMap`'s iterator throws `ConcurrentModificationException`, `ConcurrentHashMap`'s is weakly consistent. Pre-sized to `maxSize` to avoid rehashing.

### LRU — doubly-linked list with sentinels
Two sentinel nodes (`dummyHead`, `dummyTail`) mean the list is never empty, so every operation has one code path (no null-checks). `onInsert` = add to head; `onAccess` = move to head (called on **every** get); `evict` = remove `dummyTail.prev`. All O(1). `size` is a tracked field.

### LFU — frequency buckets with decay
- `TreeMap<Integer, LinkedHashSet<K>>` — frequency bucket → keys; `firstKey()` = minimum frequency = eviction target (O(log b)). `LinkedHashSet` gives an insertion-order LRU tiebreak within a bucket.
- `HashMap<K,Integer> keyToBucket` — find a key's current bucket to move it.
- `HashMap<K,LFUCacheEntry> entryIndex` — turn a victim key back into its entry.
- **Decay:** on access, `frequency = frequency*0.9 + 1.0`, bounded at `1/(1-0.9)=10`. This ages out stale popularity (prevents "frequency pollution"). Effective complexity ≈ O(1) since decay caps distinct buckets at ~10.

### StripedCache — the router (a plain array)
Just 32 partitions + a routing function `partitions[(key.hashCode() & 0x7FFFFFFF) % 32]`. `& 0x7FFFFFFF` clears the sign bit (Java's `%` on a negative is negative). No lock of its own — pure dispatch.

### CachePartition — where they stay in sync
Every operation locks once, then mutates **both** the map and the eviction structure atomically. **Invariant:** a key is in *both* structures or *neither* — never one without the other. That's why both live under the same write lock.

---

## Consistent Hashing

Implemented in `ConsistentHashRing` + `MurmurHash3`.

- **The ring** is a circle over the full 64-bit range. Nodes and keys are hashed onto it; a key belongs to the **first node clockwise** — `TreeMap.ceilingEntry(hash)`, wrapping to `firstEntry()` past the end.
- **Why consistent hashing, not `hash % N`?** With `% N`, changing N remaps almost every key (cache-wide miss storm). Consistent hashing moves only ~1/N of keys.
- **Virtual nodes (200/node).** Placing each physical node once produces clumpy, uneven arcs. Placing it 200 times (`hash("node#0"…"node#199")`) scatters it into small arcs → ~1/N each. Load-imbalance shrinks ~`1/√V`, so 200 lands at ~3% — the knee of the curve (100 ≈ 7%, 500 ≈ 2%).
- **Why MurmurHash, not `hashCode()`?** `hashCode()` is polynomial — `node#0`, `node#1`… produce consecutive values that cluster in one spot, defeating vnodes. MurmurHash's avalanche property scatters them, and it's deterministic across JVMs, so **every node builds an identical ring and agrees on ownership without any coordination.**
- **Why 64-bit?** ~600 vnodes over a 32-bit space risks position collisions (one vnode silently overwrites another → lost arc). 64 bits makes collisions statistically impossible.

---

## Concurrency Model

- **Lock striping.** 32 partitions = 32 independent locks. Two keys in different partitions never contend; collision probability for two random keys ≈ `1/32` ≈ 3%. The stripe count is a small multiple above typical core counts — enough that lock contention (not the CPU) stops being the bottleneck, while keeping each partition large enough that per-partition eviction stays smooth.
- **`get()` takes the WRITE lock.** In an LRU cache a *read is a write* — every `get` calls `moveToHead`, mutating the linked list (8 pointer writes). `prev`/`next` aren't volatile, so there's no safe concurrent read of a list mid-rewire. Only `size()` uses the read lock.
- **Why `ReentrantReadWriteLock`?** `synchronized`/`ReentrantLock` are always exclusive (no read path). `StampedLock` is faster for optimistic reads but is non-reentrant and pointless when reads mutate state. RWLock keeps a genuine read-only path for `size()` and the door open to approximate-LRU.
- **Visibility without volatile.** `CacheEntry.value`/`prev`/`next` aren't volatile — a fence on every hot-path read would hurt. The lock's `unlock()` publishes writes to the next acquirer. `running` flags *are* volatile (visibility without a lock, no CAS needed).

---

## Expiry / TTL

TTL is stored as **duration + creation time** (`ttlMs`, `createdAt`), not a deadline, keeping a complete record for serialization/replication. `isExpired() = ttlMs > 0 && (now - createdAt) > ttlMs`.

Two mechanisms, because each alone has a gap:
- **Lazy expiry (on read)** — every `get` checks TTL and evicts on the spot → never serves stale data. Gap: a key never read again would leak.
- **`TTLReaper` (background daemon)** — sweeps all 32 partitions every 1s (`scheduleAtFixedRate`), reclaiming memory from expired-but-unread keys.

**Gotcha handled:** `ScheduledExecutorService` *silently cancels all future runs* if a task throws — so `reap()` catches every `Exception` and continues. Otherwise you'd get a slow memory leak with no error anywhere.

---

## The Wire Protocol

TCP is a **byte stream with no message boundaries**, so the protocol uses **length-prefixed framing**: a fixed header carries the sizes, and a per-connection state machine (`READING_HEADER → READING_BODY`) reassembles fragmented reads.

**Request — 12-byte header + body:**
```
[magic:2][cmd:1][keyLen:2][valLen:4][ttl:2][reserved:1] │ [key bytes][value bytes]
 0xCAFE   GET/     unsigned  int      secs   0x00           UTF-8      raw bytes
          SET/
          DELETE
```

**Response — 5-byte header + body:**
```
[status:1][valLen:4] │ [value bytes]
 OK/MISS/ERROR
```

- **Magic `0xCAFE`** rejects non-protocol junk (port scanners, stray HTTP) instead of misparsing it.
- **TTL is in seconds** on the wire (the field is 2 bytes; ms would cap at 65s, seconds gives ~18h). The server multiplies by 1000.
- **Why binary, not HTTP?** An HTTP header is hundreds of text bytes and costly to parse; this is a fixed 12 bytes. Tradeoff: not human-readable and no off-the-shelf tooling — worth it for an internal cache where latency is the point. The `reserved` byte is a seam for a future request-ID (pipelining/multiplexing).
- **`MISS` vs `ERROR`** are distinct: MISS = "key doesn't exist"; ERROR = "couldn't determine" (e.g. owner unreachable). Returning MISS on a network failure would let a client cache a false negative.

---

## Cluster Mode & Transparent Forwarding

A client can connect to **any** node and get the right answer for **any** key. This works because **every node builds an identical ring** (deterministic MurmurHash + same static member list), so all nodes agree on ownership with zero coordination.

```
client ─► Node A: router.get(key) → owner = ring.getNode(key)
              owner == me?  yes → serve from local StripedCache
                            no  → forward one hop over a pooled CacheClient to the owner,
                                  relay the reply back to the client
```

- **One hop max** — the owner always concludes "I'm local" (identical ring), so it never re-forwards. (This is why `ClusterConfig` refuses to start if a node isn't in its own member list — otherwise it would forward forever.)
- **What's stored to route:** the ring (`position → node`), an address map (`node → host:port`), and a connection pool (`node → CacheClient`, created lazily via `computeIfAbsent`).
- **Failure handling:** a dead owner → `IOException` → evict the dead client from the pool (next request reconnects) → return **ERROR, not MISS**. It deliberately does *not* fall back to the local cache (that would return null for data that exists elsewhere).
- **Membership is static** (config-driven) to keep the focus on routing; production would use gossip or a coordination service.

---

## Project Layout

```
src/main/java/org/cache/
├── CacheNode.java             # owns & wires all components; lifecycle
├── CacheConfig.java           # immutable config (Builder)
├── Main.java                  # single-node entry point
├── SmokeTest.java             # end-to-end correctness test
├── LocalClusterDemo.java      # 3-node cluster in one JVM (demo)
├── ClusterMain.java           # one cluster node per process
├── LatencyBenchmark.java      # end-to-end load test (p99 over TCP)
├── core/
│   ├── StripedCache.java      # 32-partition router
│   ├── CachePartition.java    # map + eviction + lock (the unit)
│   ├── CacheEntry.java        # the shared node (key/value/ttl/DLL ptrs)
│   ├── LFUCacheEntry.java     # + frequency, lastAccessedAt
│   ├── EvictionStrategy.java  # Strategy interface
│   ├── LRUEvictionStrategy.java   # DLL + sentinels
│   ├── LFUEvictionStrategy.java   # freq buckets + decay
│   ├── CacheEntryFactory.java / DefaultEntryFactory / LFUEntryFactory
│   └── EvictionPolicy.java    # LRU | LFU
├── hash/
│   ├── ConsistentHashRing.java
│   └── MurmurHash3.java
├── cluster/
│   ├── ClusterConfig.java
│   └── ClusterRouter.java
├── protocol/
│   ├── Command.java (GET/SET/DELETE)
│   ├── Response.java (OK/MISS/ERROR)
│   └── BinaryCodec.java       # encode/decode + framing constants
├── server/
│   ├── NIOServer.java         # selector event loop
│   └── ConnectionHandler.java # per-connection parse state machine
├── client/
│   └── CacheClient.java       # blocking TCP client (also inter-node transport)
└── expiry/
    └── TTLReaper.java         # background expiry sweep

src/jmh/java/cache/CacheBenchmark.java   # JMH microbenchmark (core ops)
```

---

## Build & Run

**Requirements:** JDK 17+ (developed on 17, verified on 21), Maven.

```bash
# Build (also produces the JMH benchmarks jar)
mvn clean package
```

**Single node** — run `org.cache.Main` (from IntelliJ, or on the classpath). Optional args: `port capacity policy`.
```bash
# args: port capacity evictionPolicy(LRU|LFU)
org.cache.Main 7001 100000 LRU
```

**3-node cluster on one machine:**
- Easiest: run `org.cache.LocalClusterDemo` (starts 3 nodes in one JVM + a client demo that proves transparent forwarding).
- Realistic: run `org.cache.ClusterMain 7001`, `... 7002`, `... 7003` in three terminals (three processes).

**Correctness test:** run `org.cache.SmokeTest`.

**Benchmarks:**
```bash
# JMH core-op microbenchmark
java -jar target/benchmarks.jar cache.CacheBenchmark -f 1

# End-to-end load test (run org.cache.LatencyBenchmark; args: connections opsPerConn keyspace writePct)
org.cache.LatencyBenchmark 100 5000 50000 10
```

---

## Benchmarks & Results

Measured at **two levels** — this is the key performance story: *the data structure is not the bottleneck; the network layer is.*

### 1. Core operations — JMH microbenchmark (in-process, no network)

`CacheBenchmark`, 8 threads, 100K keyspace, ~1.5M samples/op:

| Op | Throughput | p50 | p99 | p99.9 |
|----|-----------|-----|-----|-------|
| `get` | **~4.9M ops/sec** | 0.7 µs | 48 µs | 137 µs |
| `put` | **~4.7M ops/sec** | 0.8 µs | 55 µs | 152 µs |

The p50 is sub-microsecond (uncontended lock + map lookup + LRU move); the p99 tail is lock contention on the striped partitions plus JIT/GC/safepoint noise.

### 2. End-to-end — load test over TCP (real server, real sockets)

`LatencyBenchmark`, 90% reads, round-trip latency over the binary protocol on loopback:

| Concurrency | Throughput | p50 | p99 | p99.9 |
|-------------|-----------|-----|-----|-------|
| 1 conn | 13K ops/s | 0.07 ms | 0.17 ms | 0.38 ms |
| 8 conns | 35K ops/s | 0.20 ms | 0.45 ms | 0.90 ms |
| 32 conns | 29K ops/s | 1.03 ms | 2.04 ms | 5.79 ms |
| 100 conns | 29K ops/s | 3.35 ms | 6.31 ms | 10.9 ms |

**Reading the results:** sub-millisecond p99 holds at low-to-moderate concurrency. Throughput plateaus around **30K ops/sec** because the server is a **single NIO selector thread** — every request is processed serially on one core, so under heavy fan-in requests queue and p99 climbs. The core cache does ~5M ops/sec; the gap to 30K is the cost of the TCP round-trip and single-threaded serialization.

**Methodology & caveats:**
- Latency is `System.nanoTime()` around each op; percentiles are computed from the full sorted sample (no sampling). JMH additionally handles JVM warmup, forking, and dead-code elimination.
- The load test runs on **loopback (127.0.0.1)** — a real TCP stack but no physical network hop, so numbers are best-case for the network.
- **Next optimization:** a worker-thread pool behind the selector (or multiple selectors sharded by connection) to lift the ~30K ops/sec ceiling.

---

## Testing Strategy

Three complementary layers:

- **`SmokeTest`** — *correctness.* put/get/update/delete, LRU eviction, TTL (lazy + reaper), 1000 requests on one connection (keep-alive), an 8KB value (multi-fragment framing), and a 50-thread × 1000-op concurrency stress (no deadlock/corruption).
- **`CacheBenchmark` (JMH)** — *core-op speed in isolation*, no network.
- **`LatencyBenchmark`** — *end-to-end behavior under concurrent load*: real p99 over TCP and the saturation point.

---

## Design Decisions & Tradeoffs (FAQ)

**Why 32 partitions?** A small multiple above typical core counts — enough locks that contention (not the CPU) stops being the bottleneck (collision ≈ 1/32 ≈ 3%), while keeping each partition large enough that per-partition eviction stays smooth. Too few → contention; too many → capacity fragmentation under skew (a hot partition evicts while others sit empty). Power of two.

**Why 200 virtual nodes?** Load imbalance shrinks ~`1/√V`, so returns flatten fast: 100 ≈ 7%, 200 ≈ 3%, 500 ≈ 2%. 200 is the knee. More vnodes also cost ring memory, `O(log(N·V))` lookups, and slower membership changes.

**Why does `get()` take a write lock?** Because an LRU read mutates the recency list (`moveToHead`). There's no safe lock-free read of a DLL mid-rewire.

**Why `ConcurrentHashMap` if there's already a lock?** For its weakly-consistent iterator — the reaper iterates while removing, which `HashMap` forbids.

**Why a binary protocol, not HTTP?** 12 fixed bytes vs hundreds of text bytes; trivial to parse. Gives up readability/tooling for speed.

**Why blocking client but non-blocking server?** The server faces thousands of connections → NIO is essential. A client's caller blocks on the result anyway → async would be complexity for no gain. Scale clients with a pool, one per thread.

**Why not fall back to the local cache when forwarding fails?** A local miss for a key owned elsewhere is a *wrong answer*; an error is honest.

**What's the consistency model?** Each key lives on exactly one node → strongly consistent per key, but no fault tolerance (a dead node loses its slice until the backing store repopulates). Acceptable for a cache.

---

## Known Limitations & Roadmap

- **No replication.** Each key on one node → node death loses its slice, and no help for hot keys. *Next step:* store each key on the primary + next N distinct nodes clockwise (the ring already exposes `getNodes(key, count)`); read from any replica.
- **Static membership.** No failure detection. *Next step:* gossip or ZooKeeper/etcd; feed changes through the ring's existing `addNode`/`removeNode`.
- **No data migration on rebalance.** Adding a node reassigns *ownership* but doesn't *move* existing data → transient misses until re-warm.
- **One connection per remote node** serializes inter-node forwards. *Fix:* pool N connections, or add a request-ID header for multiplexing.
- **Single NIO thread** caps throughput at ~30K ops/sec (see benchmarks). *Fix:* worker pool behind the selector.

---

*Built as a learning project to understand the internals of distributed caches — consistent hashing, lock striping, NIO event loops, and binary protocol framing — the same techniques used by Redis Cluster, Cassandra, and Memcached.*

---

## License

Released under the [MIT License](LICENSE).
