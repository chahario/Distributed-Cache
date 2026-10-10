package org.cache.protocol;

/**
 * Status codes for cache responses.
 *
 * OK:   command succeeded
 *   GET  → value found, returned in body
 *   SET  → stored successfully, no body
 *   DEL  → deleted successfully, no body
 *
 * MISS: key not found
 *   GET  → key does not exist or has expired
 *   DEL  → key did not exist (idempotent, not an error)
 *
 * ERROR: command failed
 *   Any command → server-side error (bad magic, unknown command, etc.)
 */
public enum Response {

    OK   ((byte) 0x00),
    MISS ((byte) 0x01),
    ERROR((byte) 0x02);

    private final byte code;

    Response(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }
}