package com.emailserver.worker;

import com.emailserver.model.WorkerNode;
import com.emailserver.protocol.InternalMessage;
import com.emailserver.util.MessageCodec;
import com.emailserver.worker.control.ControlServer;
import com.emailserver.worker.pop3.Pop3Server;
import com.emailserver.worker.smtp.SmtpServer;
import com.emailserver.worker.store.MailboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.UUID;

/**
 * Worker node entry point.
 *
 * Usage: java -jar worker.jar <smtpPort> <pop3Port> <controlPort> <proxyHost> <proxyControlPort>
 *
 * This class does three things:
 *   1. Starts the SMTP, POP3, and control servers
 *   2. Registers with the proxy (push-based service discovery)
 *   3. Installs a shutdown hook for graceful termination
 *
 * By accepting ports as command-line arguments, you can run multiple
 * worker instances on the same machine by giving each a different port set.
 * This is exactly how you'd test dynamic scaling: start a worker, watch
 * the proxy detect it, start another, watch load redistribute.
 */
public class WorkerMain {
    private static final Logger log = LoggerFactory.getLogger(WorkerMain.class);

    public static void main(String[] args) throws Exception {
        // Default ports if no args — convenient for single-node testing
        int smtpPort       = args.length > 0 ? Integer.parseInt(args[0]) : 2525;
        int pop3Port       = args.length > 1 ? Integer.parseInt(args[1]) : 1100;
        int controlPort    = args.length > 2 ? Integer.parseInt(args[2]) : 9090;
        String proxyHost   = args.length > 3 ? args[3] : "localhost";
        int proxyCtrlPort  = args.length > 4 ? Integer.parseInt(args[4]) : 8888;
        String workerId    = "worker-" + UUID.randomUUID().toString().substring(0, 8);

        log.info("Starting worker {} on SMTP:{} POP3:{} CONTROL:{}", workerId, smtpPort, pop3Port, controlPort);

        // Shared mailbox store — all three servers share the same in-memory store.
        // This is safe because MailboxStore uses ConcurrentHashMap and synchronized
        // lists internally. A real system would swap this for a persistent store.
        MailboxStore store = new MailboxStore();

        SmtpServer    smtp    = new SmtpServer(smtpPort, store);
        Pop3Server    pop3    = new Pop3Server(pop3Port, store);
        ControlServer control = new ControlServer(controlPort, store);

        smtp.start();
        pop3.start();
        control.start();

        log.info("Worker {} started successfully", workerId);

        // Register with the proxy — push-based discovery.
        // We try once at startup. If registration fails (proxy not up yet),
        // the operator would need to restart the worker or retry manually.
        // A production system would implement retry with exponential backoff.
        registerWithProxy(workerId, proxyHost, proxyCtrlPort, smtpPort, pop3Port, controlPort);

        // Shutdown hook: registers a callback that the JVM runs when it receives
        // SIGTERM or SIGINT (Ctrl+C). This ensures servers shut down gracefully
        // instead of being killed mid-connection. This is the Java equivalent
        // of registering signal handlers in C++.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping worker {}", workerId);
            smtp.stop();
            pop3.stop();
            control.stop();
            log.info("Worker {} stopped cleanly", workerId);
        }, "shutdown-hook"));

        // Block the main thread — the servers run on daemon threads, so without
        // this, the JVM would exit immediately after main() returns.
        // Thread.sleep(Long.MAX_VALUE) is a common pattern for "run until killed."
        log.info("Worker {} running. Press Ctrl+C to stop.", workerId);
        Thread.currentThread().join(); // blocks until interrupted
    }

    /**
     * Sends a RegisterRequest to the proxy's control port.
     *
     * This is the moment of "joining the cluster" — the worker announces
     * itself and the proxy adds it to the routing pool. In your C++ version,
     * gRPC handled the connection establishment. Here we're doing it manually:
     * open a TCP socket, send one JSON message, read the response.
     *
     * Notice that we use a try-with-resources that closes the socket after
     * registration. Registration is a one-shot operation — we don't need to
     * keep this socket open. The proxy will open its OWN persistent connection
     * back to our control server for ongoing health checks and routing.
     */
    private static void registerWithProxy(String workerId, String proxyHost, int proxyCtrlPort,
                                          int smtpPort, int pop3Port, int controlPort) {
        try (Socket socket = new Socket(proxyHost, proxyCtrlPort);
             var writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            InternalMessage.RegisterRequest reg = new InternalMessage.RegisterRequest(
                    workerId, "localhost", smtpPort, pop3Port, controlPort);

            writer.println(MessageCodec.encode(reg));

            // Read the proxy's acknowledgment
            String response = reader.readLine();
            if (response != null) {
                InternalMessage resp = MessageCodec.decode(response);
                if (resp instanceof InternalMessage.SuccessResponse s) {
                    log.info("Successfully registered with proxy: {}", s.message);
                } else if (resp instanceof InternalMessage.ErrorResponse e) {
                    log.error("Proxy rejected registration: {}", e.error);
                }
            }

        } catch (IOException e) {
            // Registration failed — proxy might not be running yet.
            // Log a warning but don't crash — the worker can still function
            // standalone for local testing.
            log.warn("Could not register with proxy at {}:{} — running standalone. ({})",
                    proxyHost, proxyCtrlPort, e.getMessage());
        }
    }
}
