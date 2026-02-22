package com.emailserver.proxy;

import com.emailserver.model.WorkerNode;
import com.emailserver.protocol.InternalMessage;
import com.emailserver.proxy.balancer.LoadBalancer;
import com.emailserver.proxy.control.ProxyControlServer;
import com.emailserver.proxy.health.HealthChecker;
import com.emailserver.proxy.registry.WorkerRegistry;
import com.emailserver.util.MessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * The reverse proxy — the single entry point for all email clients.
 *
 * Usage: java -jar proxy.jar [smtpPort] [pop3Port] [controlPort]
 *   smtpPort    — port clients connect to for sending email     (default: 25)
 *   pop3Port    — port clients connect to for reading email     (default: 110)
 *   controlPort — port workers register on                      (default: 8888)
 *
 * The proxy listens on the standard SMTP and POP3 ports. When a client
 * connects, the proxy doesn't actually speak the full email protocol itself —
 * instead it immediately proxies the TCP connection to a selected worker node.
 * The worker handles the protocol conversation, the proxy just pipes bytes.
 *
 * This is called a "transparent TCP proxy" or "layer-4 proxy" (operating at
 * the TCP layer, not the application layer). Nginx in stream mode, HAProxy,
 * and AWS Network Load Balancer all work this way. It's simpler than a
 * layer-7 proxy (which would parse the application protocol) and it means
 * we don't need to implement SMTP/POP3 in the proxy at all — the worker
 * owns that logic.
 *
 * The data flow for an SMTP connection:
 *   Client → (TCP) → Proxy → (TCP) → Worker SMTP port
 *   Proxy opens two connections and copies bytes between them bidirectionally.
 */
public class ProxyMain {
    private static final Logger log = LoggerFactory.getLogger(ProxyMain.class);

    public static void main(String[] args) throws Exception {
        int smtpPort    = args.length > 0 ? Integer.parseInt(args[0]) : 2525;  // 25 needs root
        int pop3Port    = args.length > 1 ? Integer.parseInt(args[1]) : 1100;  // 110 needs root
        int controlPort = args.length > 2 ? Integer.parseInt(args[2]) : 8888;

        log.info("Starting proxy — SMTP:{} POP3:{} CONTROL:{}", smtpPort, pop3Port, controlPort);

        // Wire up the core components.
        WorkerRegistry registry    = new WorkerRegistry();
        LoadBalancer   balancer    = new LoadBalancer(registry, LoadBalancer.Strategy.ADAPTIVE);
        HealthChecker  healthCheck = new HealthChecker(registry);

        // Start the control server first — workers may already be trying to register.
        ProxyControlServer controlServer = new ProxyControlServer(controlPort, registry);
        controlServer.start();

        // Start the health checker — it will promote STARTING workers to HEALTHY.
        healthCheck.start();

        // Start the SMTP and POP3 proxy listeners.
        startTcpProxy("SMTP", smtpPort, balancer, true);
        startTcpProxy("POP3", pop3Port, balancer, false);

        // Status reporter — logs cluster state every 15 seconds.
        // In a real system this would expose a /health HTTP endpoint instead.
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "status-reporter");
            t.setDaemon(true);
            return t;
        }).scheduleAtFixedRate(() -> {
            log.info("Cluster status — total:{} healthy:{} strategy:{}",
                    registry.getTotalWorkerCount(),
                    registry.getHealthyWorkerCount(),
                    balancer.getStrategy());
            registry.getHealthyWorkers().forEach(w ->
                    log.info("  └─ {} connections={} score={:.2f} responseTime={}ms",
                            w.getId(), w.getActiveConnections(), w.loadScore(), w.getLastResponseTimeMs()));
        }, 15_000, 15_000, java.util.concurrent.TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Proxy shutting down...");
            healthCheck.stop();
            controlServer.stop();
            log.info("Proxy stopped");
        }, "shutdown-hook"));

        log.info("Proxy running. Waiting for workers to register on port {}...", controlPort);
        Thread.currentThread().join();
    }

    /**
     * Starts a TCP proxy listener. For every incoming client connection:
     *   1. Ask the load balancer to select a worker
     *   2. Open a TCP connection to that worker's protocol port
     *   3. Pipe bytes bidirectionally until either side closes
     *
     * The "pipe bytes" approach is the key design choice. We don't parse
     * the protocol — we just forward raw bytes. This means:
     *   + The proxy is protocol-agnostic (works for SMTP, POP3, or anything)
     *   + It's very fast — minimal CPU overhead per byte
     *   - We can't make routing decisions based on protocol content
     *     (e.g., we can't read the RCPT TO address to route to a specific worker)
     *
     * For sticky routing (same user always goes to same worker), a layer-7
     * proxy that reads the POP3 USER command would be needed. That's a great
     * next step to implement if you want to extend this project.
     *
     * @param smtpMode if true, connects to worker's SMTP port; false = POP3 port
     */
    private static void startTcpProxy(String name, int listenPort,
                                       LoadBalancer balancer, boolean smtpMode) {
        Thread listener = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(listenPort)) {
                log.info("{} proxy listening on port {}", name, listenPort);

                while (!server.isClosed()) {
                    Socket clientSocket = server.accept();
                    clientSocket.setTcpNoDelay(true);

                    // Select a worker — this is where the load balancing happens.
                    Optional<WorkerNode> worker = balancer.selectWorker();

                    if (worker.isEmpty()) {
                        // No workers available — send back a protocol error and close.
                        sendProtocolError(clientSocket, name);
                        continue;
                    }

                    WorkerNode selectedWorker = worker.get();
                    int targetPort = smtpMode ? selectedWorker.getSmtpPort() : selectedWorker.getPop3Port();

                    // Connect to the worker asynchronously so accept() can handle
                    // the next client immediately without waiting for this connection.
                    WorkerNode finalWorker = selectedWorker;
                    Thread.ofVirtual().name("proxy-pipe-" + name).start(() ->
                            pipeConnection(clientSocket, finalWorker, targetPort, balancer, name));
                }
            } catch (IOException e) {
                log.error("{} proxy listener error: {}", name, e.getMessage());
            }
        }, name + "-proxy-listener");
        listener.setDaemon(true);
        listener.start();
    }

    /**
     * Bidirectional byte piping between client and worker sockets.
     *
     * We launch two threads: one copies bytes from client→worker, the
     * other copies bytes from worker→client. Both run until the connection closes.
     *
     * Thread.ofVirtual() uses Java 21's virtual threads (Project Loom).
     * Virtual threads are extremely lightweight (< 1KB overhead each, vs ~1MB
     * for platform threads), making them ideal for I/O-bound tasks like piping.
     * This is what allows the proxy to handle thousands of simultaneous connections
     * without the thread explosion problem described in SmtpServer.
     *
     * If you're familiar with Go's goroutines — virtual threads are Java's answer
     * to the same problem. In your C++ version, you likely used async I/O or a
     * thread pool. Virtual threads give you the simplicity of blocking code with
     * the scalability of async code.
     */
    private static void pipeConnection(Socket clientSocket, WorkerNode worker,
                                        int workerPort, LoadBalancer balancer, String proto) {
        try (clientSocket;
             Socket workerSocket = new Socket()) {

            workerSocket.setTcpNoDelay(true);
            workerSocket.connect(new InetSocketAddress(worker.getHost(), workerPort), 3000);

            log.debug("Proxying {} connection: client={} → worker={}:{}",
                    proto, clientSocket.getRemoteSocketAddress(), worker.getHost(), workerPort);

            InputStream  clientIn  = clientSocket.getInputStream();
            OutputStream clientOut = clientSocket.getOutputStream();
            InputStream  workerIn  = workerSocket.getInputStream();
            OutputStream workerOut = workerSocket.getOutputStream();

            // client → worker pipe
            Thread t1 = Thread.ofVirtual().start(() -> pipe(clientIn, workerOut, clientSocket, workerSocket));
            // worker → client pipe
            Thread t2 = Thread.ofVirtual().start(() -> pipe(workerIn, clientOut, workerSocket, clientSocket));

            // Wait for both pipes to finish before releasing the worker counter.
            t1.join();
            t2.join();

        } catch (Exception e) {
            log.warn("Proxy pipe error ({}): {}", proto, e.getMessage());
        } finally {
            // Critical: release the connection counter so the load balancer
            // doesn't think this worker is more loaded than it really is.
            balancer.releaseWorker(worker);
        }
    }

    /**
     * Copies bytes from src to dst until EOF or an exception.
     * When done, closes the "other" socket (the destination) to signal
     * that no more data will flow in that direction, which causes the
     * other pipe thread to also finish. This ensures clean shutdown
     * when either side closes the connection.
     */
    private static void pipe(InputStream src, OutputStream dst,
                              Socket srcSocket, Socket dstSocket) {
        byte[] buf = new byte[8192]; // 8KB buffer — a common page-aligned choice
        try {
            int n;
            while ((n = src.read(buf)) != -1) {
                dst.write(buf, 0, n);
                dst.flush();
            }
        } catch (IOException e) {
            // Expected when connection closes — not an error.
        } finally {
            try { dstSocket.shutdownOutput(); } catch (IOException ignored) {}
        }
    }

    private static void sendProtocolError(Socket socket, String proto) {
        try (socket; var writer = new PrintWriter(socket.getOutputStream())) {
            if (proto.equals("SMTP")) {
                writer.println("421 Service temporarily unavailable — no workers available");
            } else {
                writer.println("-ERR Service temporarily unavailable");
            }
        } catch (IOException ignored) {}
    }
}
