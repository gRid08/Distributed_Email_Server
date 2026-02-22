package com.emailserver.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a worker node as seen by the proxy.
 *
 * This is the entry in the proxy's "whiteboard" — the live registry of
 * nodes available to handle requests. It tracks both the worker's
 * network address and its current health/load state.
 *
 * Notice we use AtomicInteger for activeConnections — this is because
 * the proxy handles many client connections concurrently on different
 * threads, and they all read/write this counter. AtomicInteger gives us
 * thread-safe increment/decrement without needing synchronized blocks,
 * using CPU-level compare-and-swap (CAS) instructions under the hood.
 */
public class WorkerNode {

    private final String id;
    private final String host;
    private final int smtpPort;
    private final int pop3Port;
    private final int controlPort; // used for internal proxy<->worker communication

    // These are mutable state — they change as the worker handles load.
    // volatile ensures changes are visible across threads immediately
    // (without it, a thread might read a stale cached value from CPU registers).
    private volatile NodeStatus status;
    private volatile Instant lastHealthCheck;
    private volatile long lastResponseTimeMs;

    // AtomicInteger is the right tool here: multiple threads increment/decrement
    // this as sessions open and close. Without atomics, you'd get lost updates.
    private final AtomicInteger activeConnections = new AtomicInteger(0);

    @JsonCreator
    public WorkerNode(
            @JsonProperty("id")          String id,
            @JsonProperty("host")        String host,
            @JsonProperty("smtpPort")    int smtpPort,
            @JsonProperty("pop3Port")    int pop3Port,
            @JsonProperty("controlPort") int controlPort) {
        this.id          = id;
        this.host        = host;
        this.smtpPort    = smtpPort;
        this.pop3Port    = pop3Port;
        this.controlPort = controlPort;
        this.status      = NodeStatus.STARTING;
        this.lastHealthCheck = Instant.now();
        this.lastResponseTimeMs = 0;
    }

    /**
     * A worker's lifecycle: it starts in STARTING, moves to HEALTHY once
     * it passes its first health check, degrades to UNHEALTHY if checks fail,
     * and is DRAINING when we want to shut it down gracefully (let existing
     * sessions finish, but accept no new ones — a pattern called "graceful shutdown").
     */
    public enum NodeStatus {
        STARTING,   // just registered, not yet health-checked
        HEALTHY,    // passing health checks, accepting traffic
        UNHEALTHY,  // failing health checks, not receiving new traffic
        DRAINING    // shutting down gracefully
    }

    // The proxy uses this score to decide where to route requests.
    // Lower score = better candidate. This is our "adaptive" metric:
    // it combines current load (connections) with recent response time.
    // A node that's fast but busy might score similarly to one that's
    // slow but idle — the proxy balances both dimensions.
    public double loadScore() {
        return activeConnections.get() * 0.7 + (lastResponseTimeMs / 100.0) * 0.3;
    }

    public int incrementConnections() { return activeConnections.incrementAndGet(); }
    public int decrementConnections() { return activeConnections.decrementAndGet(); }
    public int getActiveConnections()  { return activeConnections.get(); }

    public String getId()              { return id; }
    public String getHost()            { return host; }
    public int getSmtpPort()           { return smtpPort; }
    public int getPop3Port()           { return pop3Port; }
    public int getControlPort()        { return controlPort; }
    public NodeStatus getStatus()      { return status; }
    public Instant getLastHealthCheck(){ return lastHealthCheck; }
    public long getLastResponseTimeMs(){ return lastResponseTimeMs; }

    public void setStatus(NodeStatus status)               { this.status = status; }
    public void setLastHealthCheck(Instant t)              { this.lastHealthCheck = t; }
    public void setLastResponseTimeMs(long ms)             { this.lastResponseTimeMs = ms; }

    @Override
    public String toString() {
        return String.format("WorkerNode{id=%s, host=%s, smtp=%d, pop3=%d, conns=%d, status=%s}",
                id, host, smtpPort, pop3Port, activeConnections.get(), status);
    }
}
