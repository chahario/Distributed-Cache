package org.cache.client;

import org.cache.protocol.BinaryCodec;
import org.cache.protocol.Command;
import org.cache.protocol.Response;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * TCP client for the distributed cache.
 *
 * Speaks the same binary protocol as NIOServer.
 * One instance = one TCP connection to one cache node.
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY BLOCKING I/O (when the server uses NIO)?
 * ─────────────────────────────────────────────────────────────────
 * Server: 1 server, 10,000 clients
 *   Thread-per-connection = 10,000 threads = ~10GB RAM
 *   → NIO with one selector thread is essential
 *
 * Client: 1 client instance, 1 connection
 *   The calling thread wants a byte[] back from get().
 *   It cannot continue until the server replies.
 *   It is going to wait regardless.
 *   → blocking I/O is simpler and equally fast
 *
 * Non-blocking client would mean returning CompletableFuture
 * or registering callbacks — enormous complexity for zero benefit
 * when the caller blocks on the result anyway.
 *
 * For high-concurrency applications: use a pool of CacheClient
 * instances (one per thread), not one non-blocking client.
 *
 * ─────────────────────────────────────────────────────────────────
 * WHY AutoCloseable?
 * ─────────────────────────────────────────────────────────────────
 * Every CacheClient holds an OS socket — a limited resource.
 * The OS has a file descriptor limit (typically 65,536 per process).
 * Leaking sockets exhausts it → "Too many open files" → crash.
 *
 * AutoCloseable enables try-with-resources:
 *
 *   try (CacheClient client = new CacheClient("localhost", 7001)) {
 *       client.set("k", v, 0);
 *   }  // close() called automatically, even if an exception is thrown
 *
 * ─────────────────────────────────────────────────────────────────
 * THREAD SAFETY
 * ─────────────────────────────────────────────────────────────────
 * NOT thread-safe. One CacheClient = one connection = one thread.
 *
 * Why? Requests and responses are correlated by ORDER, not by ID.
 * Our protocol has no request ID field.
 *
 *   Thread A: writes GET "user:1"
 *   Thread B: writes GET "user:2"   ← interleaved on the same socket
 *   Thread A: reads response         ← might get user:2's value!
 *
 * Two safe options:
 *   1. One CacheClient per thread (simple)
 *   2. A connection pool handing out clients (production)
 *
 * A pipelined multi-threaded client would require adding a
 * request-ID field to the protocol header (that's what the
 * reserved byte could become).
 */
public class CacheClient implements AutoCloseable {

    private final SocketChannel channel;
    private final String host;
    private final int port;

    /**
     * Reusable response header buffer.
     *
     * Allocated once per client, reused for every response.
     * Response header is always exactly 5 bytes:
     *   [status:1][valueLen:4]
     *
     * Why reuse instead of allocating per request?
     *   At 100K requests/sec, per-request allocation =
     *   100K ByteBuffer objects/sec → GC pressure.
     *   One reused buffer → zero allocation on the hot path.
     *
     * Safe to reuse because this class is single-threaded.
     */
    private final ByteBuffer responseHeader =
            ByteBuffer.allocate(BinaryCodec.RESPONSE_HEADER_SIZE);

    // ─────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────

    /**
     * Connects to a cache node.
     *
     * SocketChannel.open(address) both opens AND connects.
     * Blocks until the TCP handshake completes or fails.
     *
     * configureBlocking(true) is the default, but we set it
     * explicitly to document the deliberate choice.
     *
     * TCP_NODELAY disables Nagle's algorithm:
     *   Nagle buffers small writes, waiting up to 40ms to batch them
     *   with more data before sending. Great for bulk transfer.
     *   Terrible for request/response — every cache GET would add
     *   up to 40ms of latency waiting for a batch that never comes.
     *   Disabling it sends our small requests immediately.
     *
     * @param host cache server hostname or IP
     * @param port cache server port
     * @throws IOException if connection fails (server down, wrong port,
     *                     firewall, host unreachable)
     */
    public CacheClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;

        this.channel = SocketChannel.open(new InetSocketAddress(host, port));
        this.channel.configureBlocking(true);

        // Disable Nagle's algorithm — critical for request/response latency
        this.channel.socket().setTcpNoDelay(true);
    }

    // ─────────────────────────────────────────────────────────────
    // Cache operations
    // ─────────────────────────────────────────────────────────────

    /**
     * Retrieves the value for a key.
     *
     * Wire sequence:
     *   → [0xCAFE][GET][keyLen][0][0][0][key bytes]
     *   ← [OK][valueLen][value bytes]     (hit)
     *   ← [MISS][0]                        (miss or expired)
     *
     * @param key the cache key
     * @return value bytes on hit, null on miss or expiry
     * @throws IOException if the connection fails mid-request
     */
    public byte[] get(String key) throws IOException {
        validateKey(key);

        ByteBuffer request = BinaryCodec.encodeRequest(
                Command.GET, key, null, 0
        );
        writeFully(request);

        return readResponse();
    }

    /**
     * Stores a key-value pair with no expiration.
     *
     * @param key   the cache key
     * @param value the payload bytes
     * @throws IOException if the connection fails
     */
    public void set(String key, byte[] value) throws IOException {
        set(key, value, 0);
    }

    /**
     * Stores a key-value pair with TTL.
     *
     * Wire sequence:
     *   → [0xCAFE][SET][keyLen][valLen][ttl][0][key][value]
     *   ← [OK][0]
     *
     * TTL IS IN SECONDS, not milliseconds.
     *
     * Why seconds on the wire?
     *   The TTL field is 2 bytes (short) = max 65,535.
     *   In milliseconds that caps at 65 seconds — useless.
     *   In seconds that gives 65,535s ≈ 18 hours — practical.
     *   The server multiplies by 1000 to get the milliseconds
     *   that CachePartition expects.
     *
     * @param key        the cache key
     * @param value      the payload bytes
     * @param ttlSeconds expiration in SECONDS, 0 = no expiration
     * @throws IOException if the connection fails
     */
    public void set(String key, byte[] value, int ttlSeconds) throws IOException {
        validateKey(key);

        if (ttlSeconds < 0 || ttlSeconds > 65535) {
            throw new IllegalArgumentException(
                    "ttlSeconds must be 0-65535 (protocol uses a 2-byte field). Got: "
                            + ttlSeconds
            );
        }

        ByteBuffer request = BinaryCodec.encodeRequest(
                Command.SET, key, value, ttlSeconds
        );
        writeFully(request);

        // Consume the OK response — must drain it from the socket,
        // otherwise it stays buffered and corrupts the next read
        readResponse();
    }

    /**
     * Deletes a key.
     *
     * Wire sequence:
     *   → [0xCAFE][DELETE][keyLen][0][0][0][key]
     *   ← [OK][0]     (key existed and was removed)
     *   ← [MISS][0]   (key was not present)
     *
     * @param key the cache key
     * @return true if the key existed and was removed
     * @throws IOException if the connection fails
     */
    public boolean delete(String key) throws IOException {
        validateKey(key);

        ByteBuffer request = BinaryCodec.encodeRequest(
                Command.DELETE, key, null, 0
        );
        writeFully(request);

        // DELETE needs the status byte, not the value —
        // readResponse() returns null for both MISS and empty OK,
        // so we read the status explicitly here
        return readStatusOnly() == Response.OK;
    }

    // ─────────────────────────────────────────────────────────────
    // Wire I/O
    // ─────────────────────────────────────────────────────────────

    /**
     * Writes the entire buffer to the socket.
     *
     * WHY A LOOP?
     * channel.write() may write FEWER bytes than the buffer holds
     * when the OS socket send buffer is full.
     *
     *   buffer has 100 bytes
     *   channel.write(buffer) returns 40   ← only 40 sent
     *   60 bytes remain unsent
     *
     * Without the loop, the server receives a truncated request,
     * waits forever for the missing bytes, and the client waits
     * forever for a response. Deadlock.
     *
     * The loop keeps writing until hasRemaining() is false.
     */
    private void writeFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer);
            if (written == 0) {
                // Socket buffer momentarily full. In blocking mode
                // this is rare; yield rather than spin the CPU.
                Thread.onSpinWait();
            }
        }
    }

    /**
     * Reads a complete response and returns the value bytes.
     *
     * Response format:
     *   [status:1][valueLen:4][value:valueLen]
     *
     * Returns null when:
     *   status == MISS   → key not found or expired
     *   status == ERROR  → server-side failure
     *   valueLen == 0    → OK with no body (SET, DELETE)
     *
     * @return value bytes, or null (see above)
     * @throws IOException if the connection fails or the server
     *                     closes mid-response
     */
    private byte[] readResponse() throws IOException {
        // ── Read the 5-byte header ────────────────────────────────
        responseHeader.clear();
        readFully(responseHeader);
        responseHeader.flip();

        byte statusByte = responseHeader.get();
        int  valueLen   = responseHeader.getInt();

        // MISS or ERROR → no body follows
        if (statusByte != Response.OK.code()) {
            return null;
        }

        // OK with no body (SET, DELETE)
        if (valueLen == 0) {
            return null;
        }

        // ── Read the value body ───────────────────────────────────
        // Allocated per response because size varies —
        // cannot reuse a fixed buffer like the header
        ByteBuffer body = ByteBuffer.allocate(valueLen);
        readFully(body);
        body.flip();

        byte[] value = new byte[valueLen];
        body.get(value);
        return value;
    }

    /**
     * Reads a response and returns only its status.
     * Used by delete(), which cares about OK vs MISS, not the value.
     *
     * Still drains any body bytes from the socket — leaving them
     * buffered would corrupt the next request's response.
     */
    private Response readStatusOnly() throws IOException {
        responseHeader.clear();
        readFully(responseHeader);
        responseHeader.flip();

        byte statusByte = responseHeader.get();
        int  valueLen   = responseHeader.getInt();

        // Drain any body so the socket is clean for the next request
        if (valueLen > 0) {
            ByteBuffer body = ByteBuffer.allocate(valueLen);
            readFully(body);
        }

        if (statusByte == Response.OK.code())   return Response.OK;
        if (statusByte == Response.MISS.code()) return Response.MISS;
        return Response.ERROR;
    }

    /**
     * Reads until the buffer is completely full.
     *
     * WHY A LOOP?
     * channel.read() may return FEWER bytes than requested —
     * TCP fragments data across packets.
     *
     *   header needs 5 bytes
     *   channel.read(header) returns 3   ← only 3 arrived so far
     *   loop again → returns 2           ← now complete
     *
     * Without the loop, we would parse a partially-filled buffer
     * and read garbage from the uninitialized bytes.
     *
     * read() == -1 means the server closed the connection
     * (sent TCP FIN). Fail loudly rather than returning
     * a half-filled buffer.
     */
    private void readFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                throw new IOException(
                        "Server closed connection while reading response from "
                                + host + ":" + port
                );
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Lifecycle and helpers
    // ─────────────────────────────────────────────────────────────

    /**
     * Closes the TCP connection and releases the OS socket.
     *
     * Called automatically by try-with-resources.
     * Always call this — leaked sockets exhaust the process
     * file-descriptor limit.
     */
    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    public boolean isConnected() {
        return channel != null && channel.isConnected() && channel.isOpen();
    }

    public String getHost() { return host; }
    public int    getPort() { return port; }

    /**
     * Validates the key before it hits the wire.
     *
     * Why validate client-side rather than letting the server reject it?
     *   Fail fast — the caller gets an immediate exception with a clear
     *   message instead of a round trip that returns a vague ERROR status.
     *
     * The 65,535 limit comes from the protocol: keyLen is a 2-byte
     * unsigned short.
     */
    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        int byteLen = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (byteLen > 65535) {
            throw new IllegalArgumentException(
                    "Key too long: " + byteLen +
                            " bytes. Protocol limit is 65535 (keyLen is a 2-byte field)."
            );
        }
    }
}