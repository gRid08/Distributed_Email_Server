package com.emailserver.proxy.balancer;

import com.emailserver.model.WorkerNode;
import com.emailserver.proxy.registry.WorkerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The LoadBalancer decides which worker node receives each incoming request.
 *
 * We implement three strategies, switchable at runtime, so you can directly
 * observe the difference in behavior:
 *
 *   ROUND_ROBIN    — rotate through workers cyclically. Simple, but blind to load.
 *   LEAST_CONN     — always pick the worker with fewest active connections.
 *   ADAPTIVE       — our custom score-based approach (the "adaptive" in your C++ version).
 *                    Combines connections and response time into a single metric.
 *
 * The "Power of Two Choices" insight:
 * Rather than examining ALL workers to find the absolute minimum, ADAPTIVE
 * picks two workers at random and sends to the better one. This gives ~90%
 * of the benefit of perfect load balancing with much less overhead. At scale
 * (thousands of workers), scanning all workers for every request becomes a
 * bottleneck itself. Two random samples is often the right trade-off.
 *
 * This is a real algorithm used by production systems:
 * Nginx uses it in their "least connections" implementation, and it was
 * famously described in the paper "The Power of Two Random Choices" (1994).
 */
public class LoadBalancer {
    private static final Logger log = LoggerFactory.getLogger(LoadBalancer.class);

    public enum Strategy { ROUND_ROBIN, LEAST_CONN, ADAPTIVE }

    private final WorkerRegistry registry;
    // volatile so strategy changes are immediately visible to all threads
    // without needing a full synchronized block (read is a single operation)
    private volatile Strategy strategy;

    // For round-robin: keeps a monotonically increasing counter.
    // Using AtomicInteger.getAndIncrement() ensures thread safety:
    // two threads calling this simultaneously get different values.
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    public LoadBalancer(WorkerRegistry registry, Strategy initialStrategy) {
        this.registry = registry;
        this.strategy = initialStrategy;
    }

    /**
     * Selects a worker for an incoming request.
     *
     * Returns Optional.empty() if no healthy workers are available.
     * The caller must handle this case — it means the system is fully down
     * and we should return an error to the client rather than hanging.
     *
     * After selection, we increment the worker's connection counter so
     * subsequent selections account for the load we're about to add.
     * This is "optimistic" accounting — we count the connection before
     * we know if it will succeed. The caller must call releaseWorker()
     * when the session ends to decrement the counter.
     */
    public Optional<WorkerNode> selectWorker() {
        List<WorkerNode> candidates = registry.getHealthyWorkers();

        if (candidates.isEmpty()) {
            log.warn("No healthy workers available — request cannot be routed");
            return Optional.empty();
        }

        WorkerNode selected = switch (strategy) {
            case ROUND_ROBIN -> selectRoundRobin(candidates);
            case LEAST_CONN  -> selectLeastConnections(candidates);
            case ADAPTIVE    -> selectAdaptive(candidates);
        };

        selected.incrementConnections();
        log.debug("Routed to worker {} (strategy={}, connections={})",
                selected.getId(), strategy, selected.getActiveConnections());
        return Optional.of(selected);
    }

    /**
     * Must be called when a session ends, to decrement the connection counter.
     * Forgetting to call this causes the worker's "load" to accumulate
     * indefinitely — it would appear increasingly busy and stop receiving traffic.
     * This is analogous to a memory leak, but for connection counts.
     */
    public void releaseWorker(WorkerNode worker) {
        worker.decrementConnections();
        log.debug("Released worker {} (connections now={})",
                worker.getId(), worker.getActiveConnections());
    }

    // ── Routing strategies ─────────────────────────────────────────────────

    private WorkerNode selectRoundRobin(List<WorkerNode> workers) {
        // Modulo gives us cyclic wrapping: 0,1,2,0,1,2,...
        // The & 0x7FFFFFFF clears the sign bit — prevents negative values
        // if the AtomicInteger overflows past Integer.MAX_VALUE (unlikely
        // in practice, but good defensive programming).
        int idx = (roundRobinIndex.getAndIncrement() & 0x7FFFFFFF) % workers.size();
        return workers.get(idx);
    }

    private WorkerNode selectLeastConnections(List<WorkerNode> workers) {
        // Linear scan — O(n) where n is number of workers.
        // For most deployments (< 100 workers), this is completely fine.
        // For massive clusters, you'd use a min-heap for O(log n) updates.
        return workers.stream()
                      .min((a, b) -> Integer.compare(a.getActiveConnections(), b.getActiveConnections()))
                      .orElseThrow(); // safe because candidates is non-empty
    }

    /**
     * Adaptive selection using "Power of Two Choices" with our composite score.
     *
     * WorkerNode.loadScore() combines connection count (70% weight) with
     * recent response time (30% weight). The exact weights are tunable —
     * in a real system you'd experiment to find what works best for your
     * workload characteristics.
     *
     * For a 1-worker cluster, we just return it directly.
     * For 2+ workers, we pick two at random and return the better one.
     */
    private WorkerNode selectAdaptive(List<WorkerNode> workers) {
        if (workers.size() == 1) return workers.get(0);

        // Pick two distinct random workers.
        int idx1 = (int)(Math.random() * workers.size());
        int idx2;
        do {
            idx2 = (int)(Math.random() * workers.size());
        } while (idx2 == idx1); // keep rolling until we get a different index

        WorkerNode a = workers.get(idx1);
        WorkerNode b = workers.get(idx2);

        // The one with the lower score (less loaded) wins.
        WorkerNode winner = a.loadScore() <= b.loadScore() ? a : b;
        log.debug("Adaptive choice: {} (score={:.2f}) vs {} (score={:.2f}) → chose {}",
                a.getId(), a.loadScore(), b.getId(), b.loadScore(), winner.getId());
        return winner;
    }

    public void setStrategy(Strategy strategy) {
        log.info("Load balancing strategy changed: {} → {}", this.strategy, strategy);
        this.strategy = strategy;
    }

    public Strategy getStrategy() { return strategy; }
}
