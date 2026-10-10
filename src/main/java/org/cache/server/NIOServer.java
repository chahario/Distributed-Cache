package org.cache.server;

import org.cache.cluster.ClusterRouter;
import org.cache.core.StripedCache;
import org.cache.protocol.BinaryCodec;
import org.cache.protocol.Command;
import org.cache.protocol.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * Non-blocking TCP server using Java NIO.
 *
 * ARCHITECTURE:
 * ─────────────────────────────────────────────────────────────────
 * ONE thread handles ALL connections via a Selector.
 * No thread-per-connection.
 *
 * Blocking I/O with 10,000 clients:
 *   10,000 threads × ~1MB stack = ~10GB RAM
 *   Massive OS context-switching overhead
 *   Most threads blocked doing nothing
 *
 * NIO with 10,000 clients:
 *   1 thread, ~1MB
 *   The OS tells us which channels are ready
 *   No context switching between connections
 *
 * EVENT LOOP:
 * ─────────────────────────────────────────────────────────────────
 *   while (running):
 *     selector.select(1000)     → block until an event or timeout
 *     for each ready key:
 *       isAcceptable → a new client connected
 *       isReadable   → a client sent bytes
 *       isWritable   → the socket can accept our response bytes
 *     iterator.remove()          → MUST remove each processed key
 *
 * PER-CONNECTION STATE:
 * ─────────────────────────────────────────────────────────────────
 * Each SelectionKey carries an "attachment" — a ConnectionHandler
 * that tracks, for that one connection:
 *   - parse state (reading header vs reading body)
 *   - bytes accumulated so far across multiple reads
 *   - the parsed command, key, value, and TTL
 *   - the response buffer waiting to be written
 *
 * This is necessary because TCP is a byte stream, not a message
 * stream — one request may arrive across several read() calls, and
 * several requests may arrive in one read() call.
 *
 * REQUEST LIFECYCLE:
 * ─────────────────────────────────────────────────────────────────
 *   1. ACCEPT  → register the channel for READ, attach a handler
 *   2. READ    → feed bytes to the handler
 *                complete request? → process → store response
 *                                  → switch interest to WRITE
 *   3. WRITE   → write response bytes
 *                fully written? → switch interest back to READ
 *   4. REPEAT  → the same connection serves the next request
 *
 * SINGLE-NODE VS CLUSTER MODE:
 * ─────────────────────────────────────────────────────────────────
 * Determined by whether a ClusterRouter was supplied.
 *
 *   router == null  → serve every request from the local cache
 *   router != null  → ask the router, which consults the hash ring
 *                     and either serves locally or forwards over TCP
 *
 * THREAD SAFETY:
 * ─────────────────────────────────────────────────────────────────
 * The event loop runs on one dedicated thread. Selector operations
 * and all ConnectionHandler state are touched only by that thread —
 * no locking needed there.
 *
 * Cache operations are thread-safe via per-partition locks, so the
 * NIO thread can call them freely while the TTLReaper thread is
 * also scanning.
 */
public class NIOServer {

    private static final Logger log = LoggerFactory.getLogger(NIOServer.class);

    /**
     * Read buffer size per read event.
     *
     * 4KB: comfortably holds a typical request (12-byte header +
     * a short key + a modest value). Larger requests simply span
     * multiple read events — the ConnectionHandler state machine
     * stitches them together.
     *
     * Allocated per read event rather than per connection:
     *   Per-connection would cost 10,000 × 4KB = 40MB for 10K clients.
     *   Per-event, one buffer is used and discarded immediately —
     *   short-lived garbage that the young-gen GC collects cheaply.
     */
    private static final int READ_BUFFER_SIZE = 4096;

    /**
     * The local cache — this node's share of the keyspace.
     *
     * Required even in cluster mode, because the router serves
     * locally-owned keys from it.
     */
    private final StripedCache<String, byte[]> cache;

    /**
     * Cluster router — null in single-node mode.
     *
     * When null:    every request is served from the local cache.
     * When present: the router decides local vs remote per key,
     *               forwarding to the owning node when needed.
     *
     * We keep BOTH cache and router references rather than only the
     * router, because single-node deployments should not pay the cost
     * of constructing a ClusterRouter and a hash ring for a cluster
     * of one, nor the per-request branch through it.
     */
    private final ClusterRouter router;

    private final int port;

    private Selector selector;
    private ServerSocketChannel serverChannel;

    /**
     * volatile: written by stop() (possibly from the shutdown-hook
     * thread), read by the NIO event loop thread on every iteration.
     * volatile guarantees the event loop sees the change promptly
     * without the cost of a lock on the hot path.
     */
    private volatile boolean running = false;

    /**
     * The dedicated NIO thread.
     * Held so stop() can join it and wait for a clean exit.
     */
    private Thread nioThread;

    // ─────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────

    /**
     * Single-node constructor — no cluster routing.
     * Every request is served from the local cache.
     *
     * Delegates to the cluster constructor with router = null so that
     * validation lives in exactly one place.
     */
    public NIOServer(StripedCache<String, byte[]> cache, int port) {
        this(cache, null, port);
    }

    /**
     * Cluster-aware constructor.
     *
     * @param cache  the local cache — required even in cluster mode,
     *               because the router serves locally-owned keys from it
     * @param router cluster router, or null for single-node mode
     * @param port   TCP port to bind
     */
    public NIOServer(StripedCache<String, byte[]> cache,
                     ClusterRouter router,
                     int port) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache cannot be null");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
        this.cache  = cache;
        this.router = router;   // may be null — single-node mode
        this.port   = port;
    }

    // ─────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────

    /**
     * Binds the port and starts the event loop on a dedicated thread.
     *
     * The selector and server channel are set up BEFORE the thread
     * starts, so a bind failure (port in use, permission denied)
     * throws IOException to the caller immediately rather than dying
     * silently inside a background thread.
     *
     * Why a background thread at all?
     *   The event loop runs until running = false. Running it on the
     *   calling thread would mean start() never returns.
     *
     * SO_REUSEADDR:
     *   After a server stops, the OS keeps the port in TIME_WAIT for
     *   roughly 60 seconds to catch stray packets from the old
     *   connection. Without SO_REUSEADDR, restarting within that
     *   window fails with "Address already in use" — painful during
     *   development and during rolling restarts in production.
     *
     * @throws IOException if the port cannot be bound
     */
    public void start() throws IOException {
        selector      = Selector.open();
        serverChannel = ServerSocketChannel.open();

        serverChannel.socket().setReuseAddress(true);
        serverChannel.bind(new InetSocketAddress(port));

        // MUST be non-blocking before registering with a selector —
        // blocking channels cannot be registered
        serverChannel.configureBlocking(false);

        // Register for ACCEPT: wake us when a client connects
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        running = true;

        nioThread = new Thread(this::eventLoop, "nio-server");
        nioThread.start();

        log.info("NIOServer started on port {} ({} mode)",
                port, (router != null) ? "cluster" : "single-node");
    }

    /**
     * Stops the server gracefully.
     *
     * Sequence:
     *   1. running = false      → the loop will exit on its next check
     *   2. selector.wakeup()    → unblock select() immediately
     *   3. nioThread.join(5s)   → wait for the loop to actually finish
     *   4. selector.close()     → closes all registered channels
     *
     * Why wakeup()?
     *   select(1000) may be blocked for up to a second. wakeup() makes
     *   it return right now, so shutdown is immediate rather than
     *   waiting out the timeout.
     *
     * Why join with a timeout?
     *   If the loop is stuck for any reason, we do not want stop() to
     *   hang forever. Five seconds is generous for an event loop that
     *   normally exits in microseconds.
     *
     * Idempotent — safe to call twice.
     */
    public void stop() {
        running = false;

        if (selector != null) {
            selector.wakeup();
        }

        if (nioThread != null) {
            try {
                nioThread.join(5000);
                if (nioThread.isAlive()) {
                    log.warn("NIO event loop did not exit within 5s");
                }
            } catch (InterruptedException e) {
                // Restore the interrupt flag so the caller can detect it
                Thread.currentThread().interrupt();
            }
        }

        try {
            if (selector != null) selector.close();
        } catch (IOException e) {
            log.warn("Error closing selector", e);
        }

        try {
            if (serverChannel != null) serverChannel.close();
        } catch (IOException e) {
            log.warn("Error closing server channel", e);
        }

        log.info("NIOServer stopped");
    }

    // ─────────────────────────────────────────────────────────────
    // Event loop — runs on nioThread
    // ─────────────────────────────────────────────────────────────

    /**
     * The core NIO event loop.
     *
     * select(1000):
     *   Blocks until at least one channel is ready, or the timeout
     *   expires. The timeout matters: without it, select() could block
     *   indefinitely after stop() sets running = false if no events
     *   arrive. wakeup() normally handles that, but the timeout is a
     *   safety net against a missed wakeup.
     *
     * iterator.remove():
     *   CRITICAL. The selector does NOT clear selectedKeys for you.
     *   Without removing each key after processing, the same events
     *   are reprocessed on every loop iteration forever — a spinning
     *   loop at 100% CPU.
     *
     * Per-connection error isolation:
     *   An exception while handling one connection closes only that
     *   connection. The loop continues serving everyone else. One
     *   malformed request must never take down the whole server.
     */
    private void eventLoop() {
        log.info("NIO event loop started");

        while (running) {
            try {
                selector.select(1000);

                // Re-check after waking — stop() may have fired
                if (!running) break;

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectedKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();   // MUST remove before processing

                    if (!key.isValid()) continue;

                    try {
                        if (key.isAcceptable()) {
                            handleAccept();
                        } else if (key.isReadable()) {
                            handleRead(key);
                        } else if (key.isWritable()) {
                            handleWrite(key);
                        }
                    } catch (Exception e) {
                        // Isolate the failure to this one connection
                        log.warn("Error handling connection — closing it", e);
                        closeConnection(key);
                    }
                }

            } catch (IOException e) {
                if (running) {
                    log.error("NIO event loop error", e);
                }
                // If !running, this is the expected exception from
                // selector.close() during shutdown — not an error
            }
        }

        log.info("NIO event loop stopped");
    }

    // ─────────────────────────────────────────────────────────────
    // Event handlers
    // ─────────────────────────────────────────────────────────────

    /**
     * Accepts a new client connection.
     *
     * accept() may return null even though the selector reported the
     * channel as acceptable — a rare race. Defensive null check.
     *
     * configureBlocking(false) is mandatory before register() —
     * a blocking channel cannot be registered with a selector.
     *
     * The ConnectionHandler attachment is how per-connection parse
     * state survives across separate READ events. The selector holds
     * it for us; we retrieve it via key.attachment().
     */
    private void handleAccept() throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        if (clientChannel == null) return;

        clientChannel.configureBlocking(false);

        // Disable Nagle's algorithm — it batches small writes and can
        // add up to 40ms of latency. Fatal for request/response.
        clientChannel.socket().setTcpNoDelay(true);

        clientChannel.register(
                selector,
                SelectionKey.OP_READ,     // interested in: bytes from the client
                new ConnectionHandler()   // attachment: per-connection parse state
        );

        log.debug("New connection from {}", clientChannel.getRemoteAddress());
    }

    /**
     * Reads available bytes and feeds them to the parse state machine.
     *
     * bytesRead == -1 means the client closed the connection (sent
     * TCP FIN). We close our side and release the socket.
     *
     * handler.feed() returns true only when a COMPLETE request has
     * been parsed. Until then we return and wait for more READ events —
     * the handler retains the partial bytes.
     *
     * ORDER MATTERS after a complete request:
     *   processRequest() first — it reads the key and value from
     *     the handler's body buffer
     *   reset() second — clears parse state (this also nulls the
     *     response field)
     *   setResponse() last — so the response survives the reset
     *
     * Doing setResponse() before reset() would wipe the response and
     * handleWrite() would hit a NullPointerException on every request.
     */
    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel     channel = (SocketChannel) key.channel();
        ConnectionHandler handler = (ConnectionHandler) key.attachment();

        ByteBuffer readBuf = ByteBuffer.allocate(READ_BUFFER_SIZE);
        int bytesRead = channel.read(readBuf);

        if (bytesRead == -1) {
            log.debug("Client disconnected: {}", channel.getRemoteAddress());
            closeConnection(key);
            return;
        }

        if (bytesRead == 0) return;   // spurious wakeup, nothing to do

        readBuf.flip();   // switch to read mode so the handler can consume it

        if (handler.feed(readBuf)) {
            // Process while the handler's body buffer still holds key/value
            ByteBuffer response = processRequest(handler);

            handler.reset();               // clear parse state
            handler.setResponse(response); // then store the response

            key.interestOps(SelectionKey.OP_WRITE);
        }
        // feed() returned false → partial request → wait for more bytes
    }

    /**
     * Writes response bytes back to the client.
     *
     * channel.write() may write FEWER bytes than the buffer holds when
     * the OS socket send buffer is full. We do NOT loop here — looping
     * would block the event loop while one slow client's buffer drains,
     * starving every other connection.
     *
     * Instead we stay in OP_WRITE. The selector fires another WRITE
     * event when the socket can accept more, and we continue from
     * where the buffer's position left off. Fully non-blocking.
     *
     * Once fully written, switch back to OP_READ so this same
     * connection can serve the next request — the equivalent of
     * HTTP keep-alive for our protocol.
     */
    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel     channel  = (SocketChannel) key.channel();
        ConnectionHandler handler  = (ConnectionHandler) key.attachment();
        ByteBuffer        response = handler.getResponse();

        if (response == null) {
            // Nothing to write — shouldn't happen, but don't NPE
            key.interestOps(SelectionKey.OP_READ);
            return;
        }

        channel.write(response);

        if (!response.hasRemaining()) {
            handler.setResponse(null);   // release the buffer for GC
            key.interestOps(SelectionKey.OP_READ);
        }
        // Bytes remain → stay in OP_WRITE, continue on the next event
    }

    // ─────────────────────────────────────────────────────────────
    // Request processing — the bridge between network and cache
    // ─────────────────────────────────────────────────────────────

    /**
     * Executes a parsed request and returns the encoded response.
     *
     * ROUTING:
     * ─────────────────────────────────────────────────────────────
     * Single-node mode (router == null):
     *   Straight to the local StripedCache.
     *
     * Cluster mode (router != null):
     *   Through ClusterRouter, which asks the consistent hash ring
     *   who owns the key. Owned by this node → the local cache.
     *   Owned by another node → forwarded over TCP to that node.
     *
     * This is what makes the cluster transparent: a client can connect
     * to ANY node and get the correct answer for ANY key.
     *
     * WHY IOException MAPS TO ERROR AND NOT MISS:
     * ─────────────────────────────────────────────────────────────
     * MISS means "this key does not exist."
     * An unreachable remote node means "we could not determine
     * whether it exists." Returning MISS would let the client treat a
     * network failure as a confirmed absence — and potentially cache
     * that false negative. ERROR tells the truth: the request failed.
     *
     * TTL UNIT CONVERSION:
     * ─────────────────────────────────────────────────────────────
     * The wire protocol carries TTL in SECONDS because the field is
     * 2 bytes — in milliseconds that would cap at 65 seconds, useless.
     * In seconds it reaches ~18 hours. The cache API takes
     * milliseconds, so we multiply here.
     *
     * The switch is exhaustive over the Command enum. Adding a new
     * command without handling it here is a compile error, not a
     * runtime surprise.
     */
    private ByteBuffer processRequest(ConnectionHandler handler) {
        try {
            return switch (handler.getCommand()) {

                case GET -> {
                    String key = handler.getKey();

                    byte[] value = (router != null)
                            ? router.get(key)     // may forward to a remote node
                            : cache.get(key);     // local only

                    yield (value != null)
                            ? BinaryCodec.encodeResponse(Response.OK,   value)
                            : BinaryCodec.encodeResponse(Response.MISS, null);
                }

                case SET -> {
                    String key   = handler.getKey();
                    byte[] value = handler.getValue();
                    long   ttlMs = handler.getTtlSeconds() * 1000L;

                    if (router != null) {
                        router.put(key, value, ttlMs);
                    } else {
                        cache.put(key, value, ttlMs);
                    }

                    yield BinaryCodec.encodeResponse(Response.OK, null);
                }

                case DELETE -> {
                    String key = handler.getKey();

                    boolean deleted = (router != null)
                            ? router.delete(key)
                            : cache.delete(key);

                    yield deleted
                            ? BinaryCodec.encodeResponse(Response.OK,   null)
                            : BinaryCodec.encodeResponse(Response.MISS, null);
                }
            };

        } catch (IOException e) {
            // Cluster mode only — the owning node is down or unreachable
            log.error("Failed to forward {} to the owning node",
                    handler.getCommand(), e);
            return BinaryCodec.encodeResponse(Response.ERROR, null);

        } catch (Exception e) {
            log.error("Error processing request: {}", handler.getCommand(), e);
            return BinaryCodec.encodeResponse(Response.ERROR, null);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Connection cleanup
    // ─────────────────────────────────────────────────────────────

    /**
     * Closes a client connection and releases its resources.
     *
     * key.cancel():
     *   Deregisters the key from the selector. Without it, the
     *   selector keeps reporting events for a channel we no longer
     *   care about.
     *
     * channel.close():
     *   Sends TCP FIN and releases the OS socket (a file descriptor).
     *   The OS limit is typically 65,536 per process — leaking sockets
     *   eventually produces "Too many open files" and the server stops
     *   accepting anything at all.
     *
     * Each step is independently wrapped so a failure in one does not
     * prevent the other from running.
     */
    private void closeConnection(SelectionKey key) {
        try {
            key.cancel();
        } catch (Exception e) {
            log.warn("Error cancelling selection key", e);
        }

        try {
            key.channel().close();
        } catch (IOException e) {
            log.warn("Error closing channel", e);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Monitoring
    // ─────────────────────────────────────────────────────────────

    public boolean isRunning()  { return running; }
    public int     getPort()    { return port; }

    /** True if this server routes through a cluster. */
    public boolean isClustered() { return router != null; }
}