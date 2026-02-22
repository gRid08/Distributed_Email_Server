package com.emailserver.proxy.health;

import com.emailserver.model.WorkerNode;
import com.emailserver.protocol.InternalMessage;
import com.emailserver.proxy.registry.WorkerRegistry;
import com.emailserver.util.MessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Periodically checks the health of every registered worker node.
 *
 * This is the component that makes your system "fault tolerant" — it's the
 * manager who notices when a worker has stopped responding and updates the
 * whiteboard accordingly. Without this, a failed worker would keep receiving
 * traffic (which would fail for clients) until someone manually intervened.
 *
 * The health check mechanism implements a simple failure detector:
 *   - Every INTERVAL milliseconds, send a HealthCheckRequest to each worker
 *   - If the worker responds within TIMEOUT milliseconds: mark HEALTHY
 *   - If it fails N consecutive times: mark UNHEALTHY (stop sending traffic)
 *   - If it fails MAX_FAILURES times: deregister it entirely
 *
 * This is called an "eventually perfect failure detector" in distributed systems
 * theory — it might temporarily declare a live node dead (false positive) or
 * take some time to notice a dead node (latency), but eventually converges
 * on the correct view. This is the best you can do in an asynchronous network
 * where you can't distinguish "crashed" from "very slow."
 *
 * The failure count threshold (UNHEALTHY_THRESHOLD) prevents flapping:
 * a node that has one slow health check doesn't immediately lose all traffic.
 * This is the same principle as circuit breakers in services like Netflix Hystrix.
 */
public class HealthChecker {
    private static final Logger log = LoggerFactory.getLogger(HealthChecker.class);

    // How often to check each worker. Every 5 seconds is typical for
    // "fast detection" without overwhelming workers with health check traffic.
    private static final long INTERVAL_MS = 5_000;

    // How long to wait for a health check response before giving up.
    // This should be well above normal network latency but below INTERVAL_MS.
    private static final int TIMEOUT_MS = 2_000;

    // After this many consecutive failures, stop routing traffic to the node.
    private static final int UNHEALTHY_THRESHOLD = 3;

    // After this many consecutive failures, remove the node from the registry.
    // Separating these thresholds allows a node to recover and rejoin without
    // manual intervention — it stays in the registry until we're really sure it's gone.
    private static final int DEREGISTER_THRESHOLD = 10;

    private final WorkerRegistry registry;
    // Track consecutive failure counts per worker
    private final Map<String, Integer> failureCounts = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler;

    public HealthChecker(WorkerRegistry registry) {
        this.registry = registry;
    }

    /**
     * Starts the health check background thread.
     *
     * ScheduledExecutorService.scheduleAtFixedRate runs our check every
     * INTERVAL_MS. This is different from scheduleWithFixedDelay — fixedRate
     * triggers at fixed wall-clock intervals regardless of how long the check
     * takes. fixedDelay waits INTERVAL_MS after the PREVIOUS check finishes.
     *
     * For health checking, fixedRate is usually better — you want consistent
     * detection latency. For jobs where you don't want overlap (long-running
     * computations), fixedDelay is safer.
     */
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "health-checker");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(this::runHealthChecks, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("Health checker started (interval={}ms, timeout={}ms, unhealthy after {} failures)",
                INTERVAL_MS, TIMEOUT_MS, UNHEALTHY_THRESHOLD);
    }

    /**
     * Checks all registered workers in parallel.
     *
     * We use a separate thread pool for the actual checks so that a slow
     * worker (taking close to TIMEOUT_MS) doesn't delay checks for other workers.
     * Without parallelism, if worker A takes 2 seconds to time out, workers B
     * and C don't get checked until 2 seconds later — your detection latency
     * scales linearly with cluster size. Parallel checks give you constant-time
     * detection regardless of cluster size.
     */
    private void runHealthChecks() {
        var workers = registry.getAllWorkers();
        if (workers.isEmpty()) return;

        // A temporary thread pool just for this check cycle.
        // We could keep a persistent pool, but creating a bounded pool
        // per-check is clean and avoids accumulating stale threads.
        ExecutorService checkPool = Executors.newFixedThreadPool(
                Math.min(workers.size(), 10));

        try {
            for (WorkerNode worker : workers) {
                checkPool.submit(() -> checkWorker(worker));
            }
            // Wait for all checks to complete before this cycle ends.
            // Without awaitTermination, the scheduler might start the next cycle
            // before this one finishes — causing overlapping check waves.
            checkPool.shutdown();
            checkPool.awaitTermination(TIMEOUT_MS + 500L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            checkPool.shutdownNow();
        }
    }

    private void checkWorker(WorkerNode worker) {
        long startMs = System.currentTimeMillis();

        try (Socket socket = new Socket()) {
            // setSoTimeout sets the read timeout — if no data arrives within
            // TIMEOUT_MS milliseconds, read() throws SocketTimeoutException.
            // Without this, the check would hang indefinitely on a crashed worker.
            socket.setSoTimeout(TIMEOUT_MS);
            socket.connect(new InetSocketAddress(worker.getHost(), worker.getControlPort()), TIMEOUT_MS);

            var writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            writer.println(MessageCodec.encode(new InternalMessage.HealthCheckRequest()));
            String response = reader.readLine();

            long elapsedMs = System.currentTimeMillis() - startMs;

            if (response != null) {
                InternalMessage msg = MessageCodec.decode(response);
                if (msg instanceof InternalMessage.HealthCheckResponse hcr && hcr.healthy) {
                    onSuccess(worker, elapsedMs, hcr);
                } else {
                    onFailure(worker, "unhealthy response");
                }
            } else {
                onFailure(worker, "no response");
            }

        } catch (Exception e) {
            onFailure(worker, e.getMessage());
        }
    }

    /**
     * Called when a worker passes its health check.
     * Updates the worker's status, response time (for adaptive routing),
     * and resets the failure counter.
     */
    private void onSuccess(WorkerNode worker, long responseTimeMs, InternalMessage.HealthCheckResponse hcr) {
        failureCounts.put(worker.getId(), 0); // reset failure streak
        worker.setLastHealthCheck(Instant.now());
        worker.setLastResponseTimeMs(responseTimeMs);

        if (worker.getStatus() != WorkerNode.NodeStatus.HEALTHY) {
            log.info("Worker {} recovered — marking HEALTHY (responseTime={}ms, emails={})",
                    worker.getId(), responseTimeMs, hcr.totalEmailsStored);
            worker.setStatus(WorkerNode.NodeStatus.HEALTHY);
        } else {
            log.debug("Worker {} healthy (responseTime={}ms)", worker.getId(), responseTimeMs);
        }
    }

    /**
     * Called when a worker fails its health check.
     * Increments the failure counter and transitions the worker's status
     * based on the configured thresholds.
     *
     * The state transitions are:
     *   HEALTHY → UNHEALTHY (at UNHEALTHY_THRESHOLD failures): stop routing traffic
     *   UNHEALTHY → deregistered (at DEREGISTER_THRESHOLD failures): remove from registry
     */
    private void onFailure(WorkerNode worker, String reason) {
        int failures = failureCounts.merge(worker.getId(), 1, Integer::sum);
        log.warn("Worker {} health check failed ({}/{}) — reason: {}",
                worker.getId(), failures, UNHEALTHY_THRESHOLD, reason);

        if (failures >= DEREGISTER_THRESHOLD) {
            log.error("Worker {} has failed {} consecutive checks — deregistering", worker.getId(), failures);
            registry.deregister(worker.getId());
            failureCounts.remove(worker.getId());

        } else if (failures >= UNHEALTHY_THRESHOLD) {
            if (worker.getStatus() == WorkerNode.NodeStatus.HEALTHY) {
                log.warn("Worker {} marked UNHEALTHY — will not receive new traffic", worker.getId());
                worker.setStatus(WorkerNode.NodeStatus.UNHEALTHY);
            }
        }
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            log.info("Health checker stopped");
        }
    }
}
