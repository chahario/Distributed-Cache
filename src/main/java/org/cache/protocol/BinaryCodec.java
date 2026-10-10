package org.cache.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Encodes and decodes our binary cache protocol.
 *
 * WHY A SEPARATE CODEC CLASS?
 * ─────────────────────────────────────────────────────────────────
 * Encoding/decoding logic is used by both:
 *   Server: decodes incoming requests, encodes responses
 *   Client: encodes requests, decodes responses
 *
 * One codec class = one place to change if protocol changes.
 * No duplication between server and client.
 *
 * ALL METHODS ARE STATIC:
 *   Codec has no state — pure functions.
 *   input bytes → output bytes, nothing stored.
 *   Thread-safe by design (no shared mutable state).
 *
 * PROTOCOL SUMMARY:
 * ─────────────────────────────────────────────────────────────────
 * Request:
 *   [magic:2][cmd:1][keyLen:2][valLen:4][ttl:2][reserved:1][key][value]
 *   Header = 12 bytes fixed
 *   Body   = keyLen + valLen bytes variable
 *
 * Response:
 *   [status:1][valLen:4][value]
 *   Header = 5 bytes fixed
 *   Body   = valLen bytes variable
 */
public class BinaryCodec {

    /**
     * Magic bytes: 0xCA 0xFE = "CAFE"
     *
     * First 2 bytes of every request.
     * Server rejects connections that don't start with this.
     * Prevents: random port scanners, HTTP requests, etc. from
     * being parsed as cache commands.
     */
    public static final short MAGIC = (short) 0xCAFE;

    /** Fixed size of request header in bytes */
    public static final int REQUEST_HEADER_SIZE = 12;

    /** Fixed size of response header in bytes */
    public static final int RESPONSE_HEADER_SIZE = 5;

    // Private constructor — all methods static, no instances
    private BinaryCodec() {}

    // ─────────────────────────────────────────────────────────────
    // Encoding (used by client to build requests,
    //           used by server to build responses)
    // ─────────────────────────────────────────────────────────────

    /**
     * Encodes a request into a ByteBuffer ready to send over TCP.
     *
     * Buffer is returned in READ mode (flipped) — ready for channel.write().
     *
     * @param command    GET, SET, or DELETE
     * @param key        the cache key (UTF-8 encoded)
     * @param value      the value bytes (null for GET and DELETE)
     * @param ttlSeconds TTL in seconds (0 = no expiration)
     * @return flipped ByteBuffer ready for channel.write()
     */
    public static ByteBuffer encodeRequest(
            Command command,
            String key,
            byte[] value,
            int ttlSeconds) {

        byte[] keyBytes  = key.getBytes(StandardCharsets.UTF_8);
        int    valueLen  = (value != null) ? value.length : 0;
        int    totalSize = REQUEST_HEADER_SIZE + keyBytes.length + valueLen;

        ByteBuffer buf = ByteBuffer.allocate(totalSize);

        // Header (12 bytes, fixed)
        buf.putShort(MAGIC);                      // 2 bytes: magic
        buf.put(command.code());                   // 1 byte:  command
        buf.putShort((short) keyBytes.length);     // 2 bytes: key length
        buf.putInt(valueLen);                      // 4 bytes: value length
        buf.putShort((short) ttlSeconds);          // 2 bytes: TTL in seconds
        buf.put((byte) 0x00);                      // 1 byte:  reserved

        // Body (variable)
        buf.put(keyBytes);
        if (value != null) buf.put(value);

        buf.flip(); // switch to read mode — ready for channel.write()
        return buf;
    }

    /**
     * Encodes a response into a ByteBuffer ready to send to client.
     *
     * @param status  OK, MISS, or ERROR
     * @param value   response value (null for SET, DELETE, MISS, ERROR)
     * @return flipped ByteBuffer ready for channel.write()
     */
    public static ByteBuffer encodeResponse(Response status, byte[] value) {
        int valueLen  = (value != null) ? value.length : 0;
        int totalSize = RESPONSE_HEADER_SIZE + valueLen;

        ByteBuffer buf = ByteBuffer.allocate(totalSize);

        // Header (5 bytes, fixed)
        buf.put(status.code());  // 1 byte:  status
        buf.putInt(valueLen);    // 4 bytes: value length

        // Body (variable)
        if (value != null) buf.put(value);

        buf.flip();
        return buf;
    }

    // ─────────────────────────────────────────────────────────────
    // Decoding helpers (used by ConnectionHandler state machine)
    // ─────────────────────────────────────────────────────────────

    /**
     * Validates magic bytes from a parsed header buffer.
     *
     * Call after reading REQUEST_HEADER_SIZE bytes into a buffer
     * and flipping it. Buffer position must be at start.
     *
     * @throws IllegalArgumentException if magic bytes are wrong
     */
    public static void validateMagic(short magic) {
        if (magic != MAGIC) {
            throw new IllegalArgumentException(
                    "Invalid magic bytes: 0x" + String.format("%04X", magic) +
                            ". Expected: 0xCAFE. " +
                            "This connection is not using our protocol."
            );
        }
    }
}