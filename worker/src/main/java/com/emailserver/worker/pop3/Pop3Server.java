package com.emailserver.worker.pop3;

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
 * POP3 server — mirrors the SmtpServer structure.
 *
 * The accept-loop + thread-pool pattern is repeated here deliberately.
 * In a real codebase you might abstract this into a generic TcpServer class
 * to avoid duplication — but seeing the pattern twice helps you internalize it.
 */
public class Pop3Server {
    private static final Logger log = LoggerFactory.getLogger(Pop3Server.class);

    private final int port;
    private final MailboxStore store;
    private final ExecutorService threadPool;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private ServerSocket serverSocket;

    public Pop3Server(int port, MailboxStore store) {
        this.port      = port;
        this.store     = store;
        this.threadPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("pop3-worker-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running.set(true);
        log.info("POP3 server listening on port {}", port);

        Thread acceptThread = new Thread(() -> {
            while (running.get() && !serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    client.setTcpNoDelay(true);
                    activeConnections.incrementAndGet();
                    threadPool.submit(() -> {
                        try {
                            new Pop3Session(client, store).run();
                        } finally {
                            activeConnections.decrementAndGet();
                        }
                    });
                } catch (IOException e) {
                    if (running.get()) log.error("POP3 accept error: {}", e.getMessage());
                }
            }
        }, "pop3-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() {
        running.set(false);
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            log.warn("Error closing POP3 server socket: {}", e.getMessage());
        }
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("POP3 server stopped");
    }

    public int getActiveConnections() { return activeConnections.get(); }
}
