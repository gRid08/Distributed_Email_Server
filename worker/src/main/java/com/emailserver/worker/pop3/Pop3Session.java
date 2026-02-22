package com.emailserver.worker.pop3;

import com.emailserver.model.Email;
import com.emailserver.worker.store.MailboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles a single POP3 client session.
 *
 * POP3 (Post Office Protocol v3, RFC 1939) has a simpler state machine
 * than SMTP, but a very important distributed systems characteristic:
 * it uses "optimistic locking" on the mailbox.
 *
 * POP3's three states:
 *
 *   AUTHORIZATION → client must authenticate (USER + PASS commands)
 *   TRANSACTION   → client can read, mark, and list emails
 *   UPDATE        → triggered by QUIT; deletions are committed and session ends
 *
 * The locking story: In a real multi-server POP3 deployment, two clients
 * could connect to two different servers and both try to download the same
 * mailbox simultaneously. Without locking, both would get the same emails
 * and both would try to delete them — creating race conditions and duplicate
 * deliveries. RFC 1939 says the server MUST prevent concurrent access to
 * the same mailbox. In our single-worker-per-mailbox design (enforced by the
 * proxy routing the same user to the same worker), this is naturally handled —
 * but it's worth understanding why the spec requires it.
 */
public class Pop3Session implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Pop3Session.class);
    private static final AtomicInteger sessionCounter = new AtomicInteger(0);

    private final Socket socket;
    private final MailboxStore store;
    private final String sessionId;

    enum State { AUTHORIZATION, TRANSACTION, UPDATE }
    private State state = State.AUTHORIZATION;

    private String authenticatedUser;
    private String pendingUser;
    private List<Email> sessionEmails; // snapshot at login time — POP3 works on a snapshot

    public Pop3Session(Socket socket, MailboxStore store) {
        this.socket    = socket;
        this.store     = store;
        this.sessionId = "POP3-" + sessionCounter.incrementAndGet();
    }

    @Override
    public void run() {
        try (socket;
             var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             var writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {

            log.info("[{}] New POP3 connection from {}", sessionId, socket.getRemoteSocketAddress());

            // POP3 greeting. +OK means the server is ready.
            // -ERR means something went wrong. Simple but effective.
            ok(writer, "emailserver POP3 server ready");

            String line;
            while ((line = reader.readLine()) != null) {
                log.debug("[{}] C: {}", sessionId, line);
                boolean shouldContinue = processCommand(line.trim(), writer);
                if (!shouldContinue) break;
            }

        } catch (IOException e) {
            // If we were in TRANSACTION state and the client disconnected
            // without QUIT, we must roll back any pending deletions.
            if (state == State.TRANSACTION && authenticatedUser != null) {
                store.rollbackDeletions(authenticatedUser);
                log.warn("[{}] Client disconnected without QUIT — deletions rolled back for {}", sessionId, authenticatedUser);
            }
        } finally {
            log.info("[{}] POP3 session closed", sessionId);
        }
    }

    /**
     * Returns false when the session should terminate.
     * Using a return value (rather than an exception) to signal "stop"
     * is cleaner — exceptions should be for exceptional situations, not
     * normal control flow.
     */
    private boolean processCommand(String line, PrintWriter writer) {
        String[] parts = line.split(" ", 2);
        String command = parts[0].toUpperCase();
        String arg     = parts.length > 1 ? parts[1].trim() : "";

        return switch (state) {
            case AUTHORIZATION -> handleAuth(command, arg, writer);
            case TRANSACTION   -> handleTransaction(command, arg, writer);
            default            -> false;
        };
    }

    // ── AUTHORIZATION state ────────────────────────────────────────────────

    private boolean handleAuth(String cmd, String arg, PrintWriter writer) {
        return switch (cmd) {
            case "USER" -> {
                pendingUser = arg;
                ok(writer, "send PASS");
                yield true;
            }
            case "PASS" -> {
                if (pendingUser == null) {
                    err(writer, "USER required first");
                    yield true;
                }
                // Simplified auth: accept any user/pass combination.
                // A real server would check against a user database.
                // For our prototype, the "authentication" is just the
                // act of naming a mailbox.
                authenticatedUser = pendingUser;
                state             = State.TRANSACTION;
                // Take a snapshot of the mailbox at login time.
                // POP3 operates on this snapshot — new emails that arrive
                // during the session are NOT visible. This is by design:
                // it prevents TOCTOU (time-of-check/time-of-use) issues.
                sessionEmails = store.fetchEmails(authenticatedUser);
                log.info("[{}] User '{}' logged in, {} emails in mailbox", sessionId, authenticatedUser, sessionEmails.size());
                ok(writer, "logged in, " + sessionEmails.size() + " messages");
                yield true;
            }
            case "QUIT" -> {
                ok(writer, "bye");
                yield false;
            }
            default -> {
                err(writer, "unknown command in AUTHORIZATION state");
                yield true;
            }
        };
    }

    // ── TRANSACTION state ──────────────────────────────────────────────────

    private boolean handleTransaction(String cmd, String arg, PrintWriter writer) {
        return switch (cmd) {
            // STAT: number of messages and total size in bytes.
            // Clients use this to decide whether to download anything.
            case "STAT" -> {
                long totalBytes = sessionEmails.stream().mapToLong(Email::sizeInBytes).sum();
                ok(writer, sessionEmails.size() + " " + totalBytes);
                yield true;
            }

            // LIST: list message numbers and sizes.
            // Optional arg: list specific message. No arg: list all.
            case "LIST" -> {
                if (arg.isEmpty()) {
                    ok(writer, sessionEmails.size() + " messages");
                    for (int i = 0; i < sessionEmails.size(); i++) {
                        writer.println((i + 1) + " " + sessionEmails.get(i).sizeInBytes());
                    }
                    writer.println("."); // multi-line responses end with a lone "."
                } else {
                    int n = parseMessageNumber(arg, writer);
                    if (n > 0) ok(writer, n + " " + sessionEmails.get(n - 1).sizeInBytes());
                }
                yield true;
            }

            // RETR: retrieve (download) a message by number.
            // This is the core operation — the client wants the email content.
            case "RETR" -> {
                int n = parseMessageNumber(arg, writer);
                if (n > 0) {
                    Email e = sessionEmails.get(n - 1);
                    ok(writer, e.sizeInBytes() + " octets");
                    // Write email in RFC 2822 format: headers, blank line, body.
                    writer.println("From: " + e.getFrom());
                    writer.println("To: " + e.getTo());
                    writer.println("Subject: " + e.getSubject());
                    writer.println("Date: " + e.getTimestamp());
                    writer.println("Message-ID: <" + e.getId() + "@emailserver>");
                    writer.println(); // blank line separates headers from body
                    writer.println(e.getBody());
                    writer.println("."); // end of multi-line response
                }
                yield true;
            }

            // DELE: mark a message for deletion.
            // Doesn't actually delete until QUIT — this is the transaction boundary.
            case "DELE" -> {
                int n = parseMessageNumber(arg, writer);
                if (n > 0) {
                    Email target = sessionEmails.get(n - 1);
                    store.markDeleted(authenticatedUser, target.getId());
                    // Update our snapshot too so subsequent LIST/STAT reflect deletion
                    sessionEmails.set(n - 1, target.markDeleted());
                    ok(writer, "message " + n + " deleted");
                }
                yield true;
            }

            // NOOP: client just wants to keep the connection alive.
            // Useful for clients that sit idle for a while.
            case "NOOP" -> {
                ok(writer, "");
                yield true;
            }

            // RSET: undo all DELE commands in this session.
            // This is the explicit rollback operation.
            case "RSET" -> {
                store.rollbackDeletions(authenticatedUser);
                sessionEmails = store.fetchEmails(authenticatedUser); // refresh snapshot
                ok(writer, "maildrop has " + sessionEmails.size() + " messages");
                yield true;
            }

            // QUIT: commit all deletions and end session.
            // This is the transaction commit point.
            case "QUIT" -> {
                state = State.UPDATE;
                store.expungeDeleted(authenticatedUser);
                ok(writer, "bye");
                yield false;
            }

            default -> {
                err(writer, "unknown command");
                yield true;
            }
        };
    }

    /**
     * Parses a message number from a POP3 command argument.
     * POP3 uses 1-based indexing (message 1 is the first message).
     * Returns -1 and sends an error if the number is invalid.
     */
    private int parseMessageNumber(String arg, PrintWriter writer) {
        try {
            int n = Integer.parseInt(arg);
            if (n < 1 || n > sessionEmails.size()) {
                err(writer, "no such message, only " + sessionEmails.size() + " messages in maildrop");
                return -1;
            }
            return n;
        } catch (NumberFormatException e) {
            err(writer, "invalid message number: " + arg);
            return -1;
        }
    }

    private void ok(PrintWriter writer, String msg) {
        String response = "+OK " + msg;
        log.debug("[{}] S: {}", sessionId, response);
        writer.println(response);
    }

    private void err(PrintWriter writer, String msg) {
        String response = "-ERR " + msg;
        log.debug("[{}] S: {}", sessionId, response);
        writer.println(response);
    }
}
