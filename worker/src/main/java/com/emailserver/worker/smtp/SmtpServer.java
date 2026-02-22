package com.emailserver.worker.smtp;

import com.emailserver.worker.store.MailboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The SMTP server listens on a TCP port and creates a new SmtpSession
 * for each incoming client connection.
 *
 * The key design decision here is using a thread pool (ExecutorService)
 * rather than spawning a raw Thread per connection. Here's why this matters:
 *
 * If you do "new Thread(session).start()" for every connection, and 1000
 * clients connect simultaneously, you create 1000 threads. Each thread
 * in the JVM consumes ~1MB of stack space by default — that's 1GB of RAM
 * just for thread stacks, and the OS will spend more time context-switching
 * between threads than doing actual work. This is called the "C10K problem"
 * (handling 10,000 concurrent connections).
 *
 * A thread pool solves this by reusing a fixed number of threads. Incoming
 * connections queue up waiting for an available thread. The pool size should
 * be tuned to your hardware — typically 2x CPU cores for I/O-bound work
 * like this.
 *
 * Note: Netty (used in production systems like Apache James) solves this
 * even more efficiently using non-blocking I/O with a small number of
 * event-loop threads. We're using blocking I/O here because it's much
 * easier to reason about, which makes it better for learning.
 */
public class SmtpServer {
    private static final Logger log = LoggerFactory.getLogger(SmtpServer.class);

    private final int port;
    private final MailboxStore store;
    private final ExecutorService threadPool;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private ServerSocket serverSocket;

    public SmtpServer(int port, MailboxStore store) {
        this.port      = port;
        this.store     = store;
        // newCachedThreadPool creates threads as needed, reuses idle ones,
        // and terminates ones that have been idle for 60 seconds.
        // Good for variable-load scenarios like email receiving.
        this.threadPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("smtp-worker-" + t.getId());
            // Daemon threads don't prevent JVM shutdown — important for
            // clean shutdown when the main thread exits.
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running.set(true);
        log.info("SMTP server listening on port {}", port);

        // The accept loop runs on its own daemon thread, freeing the main
        // thread to start the other servers (POP3, control).
        Thread acceptThread = new Thread(() -> {
            while (running.get() && !serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    // TCP_NODELAY disables Nagle's algorithm — important for
                    // interactive protocols like SMTP where you want each
                    // response sent immediately rather than buffered.
                    client.setTcpNoDelay(true);
                    activeConnections.incrementAndGet();

                    threadPool.submit(() -> {
                        try {
                            new SmtpSession(client, store).run();
                        } finally {
                            activeConnections.decrementAndGet();
                        }
                    });

                } catch (IOException e) {
                    if (running.get()) {
                        log.error("SMTP accept error: {}", e.getMessage());
                    }
                }
            }
        }, "smtp-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() {
        running.set(false);
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            log.warn("Error closing SMTP server socket: {}", e.getMessage());
        }
        // Graceful shutdown: wait up to 30 seconds for active sessions to finish.
        // This is the "draining" pattern — stop accepting new work, let current
        // work complete. Without this, you'd interrupt sessions mid-transfer.
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("SMTP thread pool did not terminate cleanly, forcing shutdown");
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("SMTP server stopped");
    }

    public int getActiveConnections() { return activeConnections.get(); }
}
