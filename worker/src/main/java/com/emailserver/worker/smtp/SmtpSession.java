package com.emailserver.worker.smtp;

import com.emailserver.model.Email;
import com.emailserver.worker.store.MailboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles a single SMTP client session on its own thread.
 *
 * SMTP is a command-response protocol with state. The conversation must
 * follow a strict sequence: EHLO → MAIL FROM → RCPT TO → DATA → QUIT.
 * Each step is a "state" in a finite state machine (FSM), and the server
 * rejects commands that arrive out of order.
 *
 * This class implements the RFC 5321 SMTP protocol (simplified).
 * The state machine looks like:
 *
 *   [CONNECTED] → (EHLO) → [GREETED] → (MAIL FROM) → [GOT_SENDER]
 *       → (RCPT TO) → [GOT_RECIPIENT] → (DATA) → [READING_DATA]
 *       → (.) → [DONE] → (QUIT) → [CLOSED]
 *
 * Each state only accepts certain commands. If a client sends DATA before
 * MAIL FROM, the server replies with a 503 "bad sequence" error.
 * This is exactly how real SMTP servers like Postfix or Exchange work.
 */
public class SmtpSession implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SmtpSession.class);
    private static final AtomicInteger sessionCounter = new AtomicInteger(0);

    private final Socket socket;
    private final MailboxStore store;
    private final String sessionId;

    // FSM state — package-private for testability
    enum State { CONNECTED, GREETED, GOT_SENDER, GOT_RECIPIENT, READING_DATA, DONE }
    private State state = State.CONNECTED;

    // Session data accumulated as commands arrive
    private String mailFrom;
    private String rcptTo;
    private String subject = "(no subject)";
    private final StringBuilder dataBuffer = new StringBuilder();

    public SmtpSession(Socket socket, MailboxStore store) {
        this.socket    = socket;
        this.store     = store;
        this.sessionId = "SMTP-" + sessionCounter.incrementAndGet();
    }

    @Override
    public void run() {
        // try-with-resources ensures the socket is closed even if an
        // exception is thrown — critical for releasing port resources.
        try (socket;
             var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             var writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {

            log.info("[{}] New SMTP connection from {}", sessionId, socket.getRemoteSocketAddress());

            // Greeting — the first thing an SMTP server sends.
            // 220 = "Service ready". The client waits for this before doing anything.
            send(writer, "220 emailserver SMTP Service Ready");

            String line;
            while ((line = reader.readLine()) != null) {
                log.debug("[{}] C: {}", sessionId, line);
                processLine(line.trim(), writer);
                if (state == State.DONE) break;
            }

        } catch (IOException e) {
            log.warn("[{}] Session ended with error: {}", sessionId, e.getMessage());
        } finally {
            log.info("[{}] SMTP session closed", sessionId);
        }
    }

    private void processLine(String line, PrintWriter writer) {
        String upper = line.toUpperCase();

        // QUIT is valid in any state — always allow graceful exit.
        if (upper.startsWith("QUIT")) {
            send(writer, "221 Bye");
            state = State.DONE;
            return;
        }

        // RSET resets the session back to GREETED state,
        // discarding any partially built message. Useful if a client
        // wants to send another email in the same TCP connection.
        if (upper.startsWith("RSET")) {
            resetSession();
            send(writer, "250 Reset OK");
            return;
        }

        switch (state) {
            case CONNECTED -> handleConnected(line, upper, writer);
            case GREETED   -> handleGreeted(line, upper, writer);
            case GOT_SENDER    -> handleGotSender(line, upper, writer);
            case GOT_RECIPIENT -> handleGotRecipient(line, upper, writer);
            case READING_DATA  -> handleData(line, writer);
            default -> send(writer, "503 Bad sequence of commands");
        }
    }

    private void handleConnected(String line, String upper, PrintWriter writer) {
        // EHLO (Extended Hello) is the modern SMTP greeting.
        // HELO is the legacy version — we accept both.
        if (upper.startsWith("EHLO") || upper.startsWith("HELO")) {
            state = State.GREETED;
            // 250 responses list server capabilities. SIZE tells clients the
            // max message size we accept (here, 10MB).
            send(writer, "250-emailserver Hello " + line.split(" ", 2)[1]);
            send(writer, "250-SIZE 10485760");
            send(writer, "250 OK");
        } else {
            send(writer, "503 Send EHLO first");
        }
    }

    private void handleGreeted(String line, String upper, PrintWriter writer) {
        if (upper.startsWith("MAIL FROM:")) {
            // Extract the email address from: MAIL FROM:<user@example.com>
            mailFrom = extractAddress(line.substring(10));
            state    = State.GOT_SENDER;
            send(writer, "250 OK sender accepted: " + mailFrom);
        } else {
            send(writer, "503 Need MAIL FROM first");
        }
    }

    private void handleGotSender(String line, String upper, PrintWriter writer) {
        if (upper.startsWith("RCPT TO:")) {
            rcptTo = extractAddress(line.substring(8));
            state  = State.GOT_RECIPIENT;
            send(writer, "250 OK recipient accepted: " + rcptTo);
        } else {
            send(writer, "503 Need RCPT TO after MAIL FROM");
        }
    }

    private void handleGotRecipient(String line, String upper, PrintWriter writer) {
        if (upper.equals("DATA")) {
            state = State.READING_DATA;
            // 354 = "Start input; end with <CRLF>.<CRLF>"
            // This tells the client to now send the raw email body,
            // terminated by a line containing just a single dot.
            send(writer, "354 Start mail input; end with <CRLF>.<CRLF>");
        } else {
            send(writer, "503 Need DATA command");
        }
    }

    private void handleData(String line, PrintWriter writer) {
        // A line with just "." signals end of message body — this is
        // the RFC 5321 "end-of-data" marker.
        if (line.equals(".")) {
            // Parse Subject from headers if present
            String rawData = dataBuffer.toString();
            for (String headerLine : rawData.split("\n")) {
                if (headerLine.toLowerCase().startsWith("subject:")) {
                    subject = headerLine.substring(8).trim();
                    break;
                }
            }

            Email email = Email.create(mailFrom, rcptTo, subject, rawData);
            store.store(email);
            log.info("[{}] Stored email {} from {} to {}", sessionId, email.getId(), mailFrom, rcptTo);

            send(writer, "250 OK message accepted: " + email.getId());
            resetSession();
            state = State.GREETED; // ready for another MAIL FROM in the same session
        } else {
            // Dot-stuffing: RFC 5321 says a line starting with "." in the body
            // is escaped as "..". We un-escape it here.
            dataBuffer.append(line.startsWith("..") ? line.substring(1) : line).append("\n");
        }
    }

    private void resetSession() {
        mailFrom    = null;
        rcptTo      = null;
        subject     = "(no subject)";
        dataBuffer.setLength(0);
        if (state != State.CONNECTED) state = State.GREETED;
    }

    /**
     * Strips angle brackets from SMTP addresses.
     * MAIL FROM:<user@example.com> → user@example.com
     */
    private String extractAddress(String raw) {
        return raw.trim()
                  .replace("<", "")
                  .replace(">", "")
                  .trim();
    }

    private void send(PrintWriter writer, String response) {
        log.debug("[{}] S: {}", sessionId, response);
        writer.println(response);
    }
}
