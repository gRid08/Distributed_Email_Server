package com.emailserver.worker.control;

import com.emailserver.model.Email;
import com.emailserver.protocol.InternalMessage;
import com.emailserver.util.MessageCodec;
import com.emailserver.worker.store.MailboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The control server is the worker's "back channel" — it only accepts
 * connections from the proxy, not from email clients.
 *
 * This is the equivalent of your gRPC service interface in the C++ version.
 * gRPC gave you a clean RPC abstraction — you defined methods like
 * StoreEmail(), FetchEmails(), HealthCheck() and the framework handled
 * serialization, connection management, and retries. Here we're implementing
 * the same contract manually, which helps you understand what gRPC was doing.
 *
 * The control protocol is simple request-response over TCP:
 *   1. Proxy sends one JSON-encoded InternalMessage (terminated by \n)
 *   2. Worker processes it and sends back one InternalMessage response
 *   3. The connection stays open (persistent) for the duration of the session
 *
 * Keeping connections persistent (rather than reconnecting for each request)
 * is important for health checks — the proxy checks workers every few seconds,
 * and the overhead of TCP handshakes would add up if we reconnected each time.
 * This is the same reason HTTP/2 (which gRPC uses) introduced connection
 * multiplexing over HTTP/1.1's one-request-per-connection model.
 */
public class ControlServer {
    private static final Logger log = LoggerFactory.getLogger(ControlServer.class);

    private final int port;
    private final MailboxStore store;
    private final ExecutorService threadPool;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong requestCount = new AtomicLong(0);
    private ServerSocket serverSocket;

    // Track when this worker started, for reporting in health checks
    private final long startTimeMs = System.currentTimeMillis();

    public ControlServer(int port, MailboxStore store) {
        this.port      = port;
        this.store     = store;
        // The control server handles far fewer connections than SMTP/POP3
        // (only the proxy connects, plus one connection per health check).
        // A small fixed-size pool is appropriate here.
        this.threadPool = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r);
            t.setName("control-handler-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running.set(true);
        log.info("Control server listening on port {}", port);

        Thread acceptThread = new Thread(() -> {
            while (running.get() && !serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    log.debug("Control connection from {}", client.getRemoteSocketAddress());
                    threadPool.submit(() -> handleControlSession(client));
                } catch (IOException e) {
                    if (running.get()) log.error("Control accept error: {}", e.getMessage());
                }
            }
        }, "control-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    /**
     * Handles a persistent control session with the proxy.
     * A single proxy connection can send many requests sequentially —
     * health checks, store requests, fetch requests — without reconnecting.
     * We loop until the connection closes (proxy disconnects or restarts).
     */
    private void handleControlSession(Socket socket) {
        try (socket;
             var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             var writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {

            String line;
            // Read one complete JSON message per line (our framing strategy).
            while ((line = reader.readLine()) != null) {
                long reqNum = requestCount.incrementAndGet();
                long startNs = System.nanoTime();

                InternalMessage request;
                try {
                    request = MessageCodec.decode(line);
                } catch (Exception e) {
                    log.warn("Failed to decode control message: {}", e.getMessage());
                    writer.println(MessageCodec.encode(new InternalMessage.ErrorResponse("invalid message format")));
                    continue;
                }

                InternalMessage response = dispatch(request);
                long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;

                log.debug("Control req #{}: {} → {} ({}ms)", reqNum,
                        request.getClass().getSimpleName(),
                        response.getClass().getSimpleName(),
                        elapsedMs);

                writer.println(MessageCodec.encode(response));
            }

        } catch (IOException e) {
            log.warn("Control session error: {}", e.getMessage());
        }
    }

    /**
     * Routes an incoming control message to the appropriate handler.
     * This is the dispatcher — equivalent to a gRPC service implementation
     * where each RPC method has its own handler. The sealed class hierarchy
     * on InternalMessage ensures the compiler catches any missing cases.
     */
    private InternalMessage dispatch(InternalMessage msg) {
        return switch (msg) {
            case InternalMessage.StoreEmailRequest req  -> handleStore(req);
            case InternalMessage.FetchEmailsRequest req -> handleFetch(req);
            case InternalMessage.DeleteEmailRequest req -> handleDelete(req);
            case InternalMessage.HealthCheckRequest req -> handleHealthCheck();
            default -> new InternalMessage.ErrorResponse("unsupported command: " + msg.getClass().getSimpleName());
        };
    }

    private InternalMessage handleStore(InternalMessage.StoreEmailRequest req) {
        try {
            Email email = Email.create(req.from, req.to, req.subject, req.body);
            store.store(email);
            return new InternalMessage.SuccessResponse("stored:" + email.getId());
        } catch (Exception e) {
            log.error("Failed to store email", e);
            return new InternalMessage.ErrorResponse("store failed: " + e.getMessage());
        }
    }

    private InternalMessage handleFetch(InternalMessage.FetchEmailsRequest req) {
        try {
            List<Email> emails = store.fetchEmails(req.mailbox);
            return new InternalMessage.FetchEmailsResponse(emails);
        } catch (Exception e) {
            log.error("Failed to fetch emails for {}", req.mailbox, e);
            return new InternalMessage.ErrorResponse("fetch failed: " + e.getMessage());
        }
    }

    private InternalMessage handleDelete(InternalMessage.DeleteEmailRequest req) {
        boolean deleted = store.markDeleted(req.mailbox, req.emailId);
        return deleted
                ? new InternalMessage.SuccessResponse("deleted")
                : new InternalMessage.ErrorResponse("email not found: " + req.emailId);
    }

    /**
     * Health check response — this is what the proxy reads to determine
     * if this worker is alive and what its current load looks like.
     * The proxy uses activeConnections and responseTimeMs to compute
     * a load score for routing decisions.
     *
     * The response time reported here is the time since the worker started,
     * as a proxy for "busy-ness" — a real system would measure actual
     * request latency using a rolling window average.
     */
    private InternalMessage handleHealthCheck() {
        long uptimeMs = System.currentTimeMillis() - startTimeMs;
        return new InternalMessage.HealthCheckResponse(
                true,                          // healthy
                0,                             // activeConnections (simplified)
                Math.min(uptimeMs / 1000, 100), // responseTimeMs proxy
                store.getTotalStored()         // total emails stored on this node
        );
    }

    public void stop() {
        running.set(false);
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            log.warn("Error closing control server socket: {}", e.getMessage());
        }
        threadPool.shutdownNow();
        log.info("Control server stopped");
    }
}
