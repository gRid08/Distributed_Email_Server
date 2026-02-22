package com.emailserver.worker.store;

import com.emailserver.model.Email;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Stores emails for all mailboxes on this worker node.
 *
 * This is an in-memory store for now — emails live as long as the JVM does.
 * A production system would persist to disk or a database. We'll keep it
 * in-memory for the prototype so you can focus on the distributed systems
 * concepts without I/O complexity.
 *
 * The key data structure choice here is ConcurrentHashMap.
 * Why not a regular HashMap? Because multiple threads — one per client
 * connection — will simultaneously read and write this map. A regular
 * HashMap is not thread-safe: concurrent modification can corrupt its
 * internal structure and cause infinite loops or data loss. You would
 * have seen the same problem in your C++ version if you used std::unordered_map
 * without a mutex.
 *
 * ConcurrentHashMap solves this with fine-grained locking: it doesn't lock
 * the entire map for each operation, but only a "segment" of it. This means
 * many threads can work on different mailboxes simultaneously — much better
 * throughput than a single global lock would provide.
 */
public class MailboxStore {
    private static final Logger log = LoggerFactory.getLogger(MailboxStore.class);

    // Map of mailbox name → list of emails.
    // The inner list is a synchronized wrapper — we'll need to lock on it
    // explicitly when doing compound operations (check-then-modify).
    private final Map<String, List<Email>> mailboxes = new ConcurrentHashMap<>();

    // AtomicLong for a simple, thread-safe email counter.
    private final AtomicLong totalStored = new AtomicLong(0);

    /**
     * Stores an email in the recipient's mailbox.
     *
     * computeIfAbsent atomically creates the mailbox list if it doesn't
     * exist. This is safer than the naive "if (!map.containsKey) map.put"
     * approach, which would have a race condition between the check and the put
     * (two threads might both see the key missing and both try to create it).
     */
    public void store(Email email) {
        // computeIfAbsent atomically creates the inbox if it doesn't exist.
        // We use synchronizedList so all add/iterate operations on the list
        // are thread-safe — ConcurrentHashMap only protects the map itself,
        // not the list values stored inside it.
        List<Email> inbox = mailboxes.computeIfAbsent(email.getTo(), k ->
                java.util.Collections.synchronizedList(new ArrayList<>()));
        inbox.add(email);
        long count = totalStored.incrementAndGet();
        log.debug("Stored email {} in mailbox '{}'. Total stored: {}", email.getId(), email.getTo(), count);
    }

    /**
     * Returns all non-deleted emails for a mailbox.
     *
     * We return a defensive copy (new ArrayList) rather than the live list.
     * If we returned the live list, the caller could modify it from outside
     * this class, or we could get ConcurrentModificationException if another
     * thread adds to the list while the caller is iterating over it.
     * Defensive copies are a simple but powerful way to enforce encapsulation.
     */
    public List<Email> fetchEmails(String mailbox) {
        List<Email> inbox = mailboxes.get(mailbox);
        if (inbox == null) return List.of(); // no mailbox = empty inbox, not an error

        synchronized (inbox) {
            return inbox.stream()
                        .filter(e -> !e.isDeleted())
                        .collect(Collectors.toList());
        }
    }

    /**
     * Marks an email as deleted. In POP3, deletion is a two-phase operation:
     * DELE marks for deletion, but emails aren't actually removed until the
     * session ends with QUIT. If the client disconnects without QUIT (e.g.,
     * network failure), deletions are rolled back. This is why we soft-delete
     * (mark a flag) rather than immediately removing from the list.
     *
     * This "mark for deletion, commit on QUIT" pattern is a simple form of
     * a transaction — the QUIT command is the commit, disconnect without QUIT
     * is the rollback. You probably implemented the same logic in C++.
     */
    public boolean markDeleted(String mailbox, String emailId) {
        List<Email> inbox = mailboxes.get(mailbox);
        if (inbox == null) return false;

        synchronized (inbox) {
            for (int i = 0; i < inbox.size(); i++) {
                if (inbox.get(i).getId().equals(emailId)) {
                    inbox.set(i, inbox.get(i).markDeleted()); // replace with deleted copy
                    log.debug("Marked email {} as deleted in mailbox '{}'", emailId, mailbox);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Actually removes deleted emails. Called when a POP3 session ends with QUIT.
     * This is the "commit" phase of the soft-delete transaction.
     */
    public void expungeDeleted(String mailbox) {
        List<Email> inbox = mailboxes.get(mailbox);
        if (inbox == null) return;

        synchronized (inbox) {
            int before = inbox.size();
            inbox.removeIf(Email::isDeleted);
            log.info("Expunged {} emails from mailbox '{}'", before - inbox.size(), mailbox);
        }
    }

    /**
     * Rolls back all pending deletions in a mailbox.
     * Called when a POP3 client disconnects abruptly (without QUIT).
     */
    public void rollbackDeletions(String mailbox) {
        List<Email> inbox = mailboxes.get(mailbox);
        if (inbox == null) return;

        synchronized (inbox) {
            inbox.replaceAll(e -> e.isDeleted() ? Email.create(e.getFrom(), e.getTo(), e.getSubject(), e.getBody()) : e);
        }
        log.info("Rolled back pending deletions for mailbox '{}'", mailbox);
    }

    public long getTotalStored() { return totalStored.get(); }

    public int getMailboxCount()  { return mailboxes.size(); }
}
