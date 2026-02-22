package com.emailserver.proxy.control;

import com.emailserver.model.WorkerNode;
import com.emailserver.protocol.InternalMessage;
import com.emailserver.proxy.registry.WorkerRegistry;
import com.emailserver.util.MessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The proxy's control server listens for incoming worker registrations.
 *
 * When a new worker starts up, it connects here and sends a RegisterRequest.
 * The proxy adds it to the registry and replies with a success acknowledgment.
 * After that, the worker serves client connections and responds to health checks
 * on its OWN control server — this proxy control server's job is done for
 * that worker until it registers again (e.g., after a restart).
 *
 * This is the "push-based service discovery" model. An alternative is
 * "pull-based" (the proxy periodically scans a known port range for new workers),
 * but push is cleaner — workers know they exist, the proxy doesn't have to guess.
 *
 * In production, tools like Consul, etcd, or ZooKeeper provide service discovery
 * as a dedicated service. Here we're implementing it manually to understand
 * what those tools are actually doing under the hood.
 */
public class ProxyControlServer {
    private static final Logger log = LoggerFactory.getLogger(ProxyControlServer.class);

    private final int port;
    private final WorkerRegistry registry;
    private final ExecutorService threadPool;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ServerSocket serverSocket;

    public ProxyControlServer(int port, WorkerRegistry registry) {
        this.port      = port;
        this.registry  = registry;
        this.threadPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("proxy-control-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running.set(true);
        log.info("Proxy control server listening on port {} for worker registrations", port);

        Thread acceptThread = new Thread(() -> {
            while (running.get() && !serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    threadPool.submit(() -> handleRegistration(client));
                } catch (IOException e) {
                    if (running.get()) log.error("Proxy control accept error: {}", e.getMessage());
                }
            }
        }, "proxy-control-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void handleRegistration(Socket socket) {
        try (socket;
             var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             var writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {

            String line = reader.readLine();
            if (line == null) return;

            InternalMessage msg = MessageCodec.decode(line);

            if (msg instanceof InternalMessage.RegisterRequest reg) {
                WorkerNode node = new WorkerNode(
                        reg.workerId, reg.host,
                        reg.smtpPort, reg.pop3Port, reg.controlPort);

                // Workers start in STARTING status. The health checker will
                // run its first check and promote them to HEALTHY if they pass.
                // This prevents sending traffic to a worker that just registered
                // but hasn't fully started up its SMTP/POP3 servers yet.
                node.setStatus(WorkerNode.NodeStatus.STARTING);
                registry.register(node);

                writer.println(MessageCodec.encode(
                        new InternalMessage.SuccessResponse("registered:" + reg.workerId)));

                log.info("Worker {} registered from {}:{} (smtp={}, pop3={}, control={})",
                        reg.workerId, reg.host, reg.controlPort,
                        reg.smtpPort, reg.pop3Port, reg.controlPort);
            } else {
                writer.println(MessageCodec.encode(
                        new InternalMessage.ErrorResponse("expected RegisterRequest")));
            }

        } catch (IOException e) {
            log.warn("Registration handling error: {}", e.getMessage());
        }
    }

    public void stop() {
        running.set(false);
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            log.warn("Error closing proxy control server: {}", e.getMessage());
        }
        threadPool.shutdownNow();
        log.info("Proxy control server stopped");
    }
}
