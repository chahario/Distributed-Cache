package org.cache.protocol;

/**
 * Commands supported by the cache protocol.
 *
 * Each command has a single-byte code sent in the request header.
 * Using an enum with fromByte() gives us:
 *   - Type safety (no raw byte comparisons scattered in code)
 *   - Single place to add new commands
 *   - Readable switch statements in the server
 */
public enum Command {

    GET   ((byte) 0x01),
    SET   ((byte) 0x02),
    DELETE((byte) 0x03);

    private final byte code;

    Command(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /**
     * Converts a raw byte from the network to a Command enum.
     *
     * @throws IllegalArgumentException if byte is not a known command
     *         Server catches this and sends ERROR response to client
     *         instead of crashing
     */
    public static Command fromByte(byte b) {
        return switch (b) {
            case 0x01 -> GET;
            case 0x02 -> SET;
            case 0x03 -> DELETE;
            default   -> throw new IllegalArgumentException(
                    "Unknown command byte: 0x" + String.format("%02X", b)
            );
        };
    }
}