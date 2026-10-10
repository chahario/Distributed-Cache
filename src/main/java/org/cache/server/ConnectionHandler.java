package org.cache.server;

import org.cache.protocol.BinaryCodec;
import org.cache.protocol.Command;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Per-connection state machine for parsing incoming requests.
 *
 * WHY A STATE MACHINE?
 * ─────────────────────────────────────────────────────────────────
 * NIO reads are PARTIAL. One request may arrive in multiple reads:
 *
 *   Request = 12-byte header + key + value
 *   Read 1:  8 bytes  → header incomplete, wait
 *   Read 2:  4 bytes  → header complete, start reading body
 *   Read 3:  body     → complete request, process it
 *
 * A state machine tracks where we are in parsing across reads.
 * Each ConnectionHandler instance belongs to ONE client connection.
 * Attached to SelectionKey as an "attachment" object.
 *
 * STATE TRANSITIONS:
 * ─────────────────────────────────────────────────────────────────
 *
 *   READING_HEADER ──(got 12 bytes)──► READING_BODY
 *        ▲                                  │
 *        │                          (got keyLen+valLen bytes)
 *        │                                  │
 *        └──────────── reset() ◄────── complete request available
 *
 * MEMORY:
 * ─────────────────────────────────────────────────────────────────
 *   headerBuf: fixed 12 bytes, reused across requests
 *   bodyBuf:   allocated per request (size = keyLen + valLen)
 *              set to null after processing → eligible for GC
 *
 * THREAD SAFETY:
 * ─────────────────────────────────────────────────────────────────
 *   Each ConnectionHandler belongs to one connection.
 *   The NIO selector processes one key at a time (single thread).
 *   No concurrent access to one ConnectionHandler → no lock needed.
 */
public class ConnectionHandler {

    /**
     * Parsing states.
     * READING_HEADER: accumulating header bytes
     * READING_BODY:   accumulating body bytes (key + value)
     */
    private enum ParseState {
        READING_HEADER,
        READING_BODY
    }

    // Current state — starts at READING_HEADER
    private ParseState state = ParseState.READING_HEADER;

    /**
     * Fixed-size header buffer — reused across requests.
     * Allocated once per connection, never GC'd.
     * 12 bytes for our request header.
     */
    private final ByteBuffer headerBuf =
            ByteBuffer.allocate(BinaryCodec.REQUEST_HEADER_SIZE);

    /**
     * Variable-size body buffer — allocated per request.
     * Size = keyLen + valLen (parsed from header).
     * Set to null after processing to allow GC.
     */
    private ByteBuffer bodyBuf;

    // ── Parsed header fields ──────────────────────────────────────
    // Set when header is fully read and parsed.
    // Used by getKey(), getValue(), getCommand(), getTtlSeconds().

    private Command command;
    private int     keyLen;
    private int     valLen;
    private int     ttlSeconds;

    /**
     * Response buffer to send back to client.
     * Set by NIOServer after processing the request.
     * Read by handleWrite() to send bytes to client.
     */
    private ByteBuffer responseBuf;

    // ─────────────────────────────────────────────────────────────
    // Core method: feed incoming bytes
    // ─────────────────────────────────────────────────────────────

    /**
     * Feeds incoming bytes into the state machine.
     *
     * Called by NIOServer every time a READ event fires for this connection.
     * May be called multiple times before a complete request is available.
     *
     * Returns true when a COMPLETE request has been parsed.
     * Returns false when more bytes are needed.
     *
     * HOW PARTIAL READ HANDLING WORKS:
     * ─────────────────────────────────────────────────────────────
     * incoming buffer may contain:
     *   - Less than we need (partial) → copy what we can, return false
     *   - Exactly what we need        → copy it all, return true/advance state
     *   - More than we need           → copy what we need, leave rest in incoming
     *                                   (next state reads the leftover)
     *
     * We use Math.min(incoming.remaining(), buf.remaining()) to copy
     * only as many bytes as BOTH sides can handle.
     *
     * @param incoming ByteBuffer containing freshly read bytes from channel
     * @return true if a complete request is ready to process
     */
    public boolean feed(ByteBuffer incoming) {

        // ── State: READING_HEADER ─────────────────────────────────
        if (state == ParseState.READING_HEADER) {

            // Copy bytes from incoming into headerBuf
            // Only copy as many as headerBuf still needs
            copyBytes(incoming, headerBuf);

            // Header not yet complete — need more bytes
            if (headerBuf.hasRemaining()) return false;

            // Header complete — parse it
            headerBuf.flip(); // switch to read mode

            short magic = headerBuf.getShort();    // bytes 0-1: magic
            BinaryCodec.validateMagic(magic);       // throws if wrong

            byte cmdByte = headerBuf.get();        // byte  2:   command
            command      = Command.fromByte(cmdByte);

            keyLen     = Short.toUnsignedInt(headerBuf.getShort()); // bytes 3-4
            valLen     = headerBuf.getInt();                         // bytes 5-8
            ttlSeconds = Short.toUnsignedInt(headerBuf.getShort()); // bytes 9-10
            headerBuf.get();  // byte 11: reserved — skip

            // If no body (GET, DELETE with no value) → complete
            int bodySize = keyLen + valLen;
            if (bodySize == 0) return true;

            // Allocate body buffer and move to next state
            bodyBuf = ByteBuffer.allocate(bodySize);
            state   = ParseState.READING_BODY;
        }

        // ── State: READING_BODY ───────────────────────────────────
        if (state == ParseState.READING_BODY) {

            copyBytes(incoming, bodyBuf);

            // Body not yet complete — need more bytes
            if (bodyBuf.hasRemaining()) return false;

            bodyBuf.flip(); // switch to read mode
            return true;    // complete request ready
        }

        return false;
    }

    // ─────────────────────────────────────────────────────────────
    // Accessors — call after feed() returns true
    // ─────────────────────────────────────────────────────────────

    /**
     * Extracts the key from the body buffer.
     * Call only after feed() returns true.
     *
     * Reads keyLen bytes from current bodyBuf position.
     * UTF-8 decode — same encoding used by client.
     */
    public String getKey() {
        byte[] keyBytes = new byte[keyLen];
        bodyBuf.get(keyBytes);
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    /**
     * Extracts the value from the body buffer.
     * Call only after feed() returns true AND after getKey().
     * (getKey() advances bodyBuf position past the key bytes)
     *
     * Returns null if valLen == 0 (GET and DELETE commands).
     */
    public byte[] getValue() {
        if (valLen == 0) return null;
        byte[] valueBytes = new byte[valLen];
        bodyBuf.get(valueBytes);
        return valueBytes;
    }

    public Command getCommand()    { return command; }
    public int     getTtlSeconds() { return ttlSeconds; }

    // ─────────────────────────────────────────────────────────────
    // Response management
    // ─────────────────────────────────────────────────────────────

    /**
     * Sets the response to send back to the client.
     * Called by NIOServer after processing the request.
     */
    public void setResponse(ByteBuffer response) {
        this.responseBuf = response;
    }

    /**
     * Returns the response buffer.
     * Called by NIOServer's handleWrite() to send bytes.
     */
    public ByteBuffer getResponse() {
        return responseBuf;
    }

    // ─────────────────────────────────────────────────────────────
    // Reset — prepare for next request on same connection
    // ─────────────────────────────────────────────────────────────

    /**
     * Resets state machine for the next request on this connection.
     *
     * Called after response is fully sent to client.
     * One TCP connection can handle many requests sequentially
     * (HTTP keep-alive equivalent for our protocol).
     *
     * headerBuf.clear(): position=0, limit=capacity
     *   Ready to accept 12 new header bytes.
     *
     * bodyBuf = null: release reference → GC can reclaim body memory
     *   Next request allocates a new bodyBuf sized for that request.
     *   Body sizes vary per request — can't reuse fixed-size buffer.
     *
     * responseBuf = null: release reference → GC can reclaim
     */
    public void reset() {
        state       = ParseState.READING_HEADER;
        headerBuf.clear();
        bodyBuf     = null;
        command     = null;
        keyLen      = 0;
        valLen      = 0;
        ttlSeconds  = 0;
    }

    // ─────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────

    /**
     * Copies bytes from src to dst, up to what both can handle.
     *
     * Why not src.get(dst.array())?
     *   dst might be partially filled already.
     *   src might have more bytes than dst needs.
     *   This method handles both cases safely.
     *
     * Math.min: copies the smaller of:
     *   src.remaining() → bytes available to copy from src
     *   dst.remaining() → space available in dst
     *
     * Temporarily limits src to prevent over-reading:
     *   save old limit → set limit = position + toRead → copy → restore limit
     */
    private void copyBytes(ByteBuffer src, ByteBuffer dst) {
        int toRead   = Math.min(src.remaining(), dst.remaining());
        int oldLimit = src.limit();

        src.limit(src.position() + toRead); // temporarily limit src
        dst.put(src);                        // copy exactly toRead bytes
        src.limit(oldLimit);                 // restore original limit
    }
}