package com.emailserver.proxy.registry;

import com.emailserver.model.WorkerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The WorkerRegistry is the proxy's live view of the cluster.
 *
 * Think of it as the "whiteboard" in a busy office — workers write their
 * name (and capabilities) when they arrive, and the manager (health checker)
 * erases names when workers stop responding.
 *
 * The registry needs to handle concurrent access safely because multiple
 * components touch it simultaneously:
 *   - The health checker thread periodically reads all workers and updates their status
 *   - The load balancer reads healthy workers for every incoming connection
 *   - The control server adds workers when they register
 *
 * We use ConcurrentHashMap which, as you saw in MailboxStore, allows safe
 * concurrent reads and writes without a global lock. For operations that
 * need to read-then-modify atomically (like "if not present, add"), we use
 * ConcurrentHashMap's atomic methods (putIfAbsent, computeIfAbsent).
 */
public class WorkerRegistry {
    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);

    // Key: worker ID, Value: WorkerNode with current status and metrics.
    private final Map<String, WorkerNode> workers = new ConcurrentHashMap<>();

    /**
     * Adds a newly registered worker to the pool.
     * putIfAbsent is atomic — safe to call from multiple threads simultaneously
     * without the risk of overwriting an existing registration.
     */
    public void register(WorkerNode node) {
        WorkerNode existing = workers.putIfAbsent(node.getId(), node);
        if (existing == null) {
            log.info("Registered new worker: {}", node);
        } else {
            log.warn("Worker {} tried to register again — ignoring duplicate", node.getId());
        }
    }

    /**
     * Removes a worker from the pool entirely.
     * This is called when a worker is confirmed permanently unreachable —
     * not just temporarily slow. The distinction matters: you don't want to
     * remove a worker just because one health check timed out (temporary glitch),
     * but you do want to remove one that has failed N consecutive checks.
     */
    public void deregister(String workerId) {
        WorkerNode removed = workers.remove(workerId);
        if (removed != null) {
            log.info("Deregistered worker: {}", removed);
        }
    }

    /**
     * Returns all workers currently marked HEALTHY.
     * The load balancer calls this to get its routing candidates.
     *
     * We return a new ArrayList (snapshot) rather than a live view of the map
     * values. This prevents the load balancer from iterating over a collection
     * that might be modified concurrently — ConcurrentModificationException is
     * not thrown by ConcurrentHashMap but the list could change mid-iteration
     * in confusing ways. A snapshot is simpler and safer.
     */
    public List<WorkerNode> getHealthyWorkers() {
        List<WorkerNode> healthy = new ArrayList<>();
        for (WorkerNode node : workers.values()) {
            if (node.getStatus() == WorkerNode.NodeStatus.HEALTHY) {
                healthy.add(node);
            }
        }
        return healthy;
    }

    /** Returns all workers regardless of status — used by the health checker. */
    public Collection<WorkerNode> getAllWorkers() {
        return new ArrayList<>(workers.values()); // snapshot
    }

    public Optional<WorkerNode> findById(String id) {
        return Optional.ofNullable(workers.get(id));
    }

    public int getTotalWorkerCount()   { return workers.size(); }
    public int getHealthyWorkerCount() { return getHealthyWorkers().size(); }
}
