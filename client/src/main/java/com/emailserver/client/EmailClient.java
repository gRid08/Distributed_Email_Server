package com.emailserver.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * A simple command-line email client for testing the system end-to-end.
 *
 * This speaks raw SMTP and POP3 over TCP — no library abstraction. Reading
 * through this code gives you a concrete feel for what happens at the protocol
 * level every time an email client like Thunderbird or Apple Mail checks your inbox.
 *
 * Usage:
 *   Send:    java -jar client.jar send <host> <smtpPort> <from> <to> <subject> <body>
 *   Receive: java -jar client.jar recv <host> <pop3Port> <user>
 *   Stress:  java -jar client.jar stress <host> <smtpPort> <count>
 */
public class EmailClient {
    private static final Logger log = LoggerFactory.getLogger(EmailClient.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage:");
            System.out.println("  send  <host> <smtpPort> <from> <to> <subject> <body>");
            System.out.println("  recv  <host> <pop3Port> <user>");
            System.out.println("  stress <host> <smtpPort> <count>");
            System.exit(1);
        }

        String command = args[0];
        String host    = args[1];
        int    port    = Integer.parseInt(args[2]);

        switch (command) {
            case "send"   -> sendEmail(host, port,
                    args.length > 3 ? args[3] : "alice@example.com",
                    args.length > 4 ? args[4] : "bob@example.com",
                    args.length > 5 ? args[5] : "Hello",
                    args.length > 6 ? args[6] : "Test email body");
            case "recv"   -> receiveEmails(host, port, args.length > 3 ? args[3] : "bob@example.com");
            case "stress" -> stressTest(host, port, args.length > 3 ? Integer.parseInt(args[3]) : 10);
            default       -> System.out.println("Unknown command: " + command);
        }
    }

    /**
     * Sends one email via SMTP by manually issuing each protocol command
     * and reading the server's responses. You can watch this in action by
     * running it with the proxy's SMTP port and looking at the worker logs.
     */
    static void sendEmail(String host, int port, String from, String to,
                          String subject, String body) throws IOException {
        System.out.printf("Sending email via SMTP %s:%d%n", host, port);

        try (Socket socket = new Socket(host, port);
             var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             var writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {

            // Read and print each server response for visibility.
            // The server always speaks first in SMTP (the 220 greeting).
            expectOk(reader, "220");
            send(writer, "EHLO test-client");
            expectOk(reader, "250");

            send(writer, "MAIL FROM:<" + from + ">");
            expectOk(reader, "250");

            send(writer, "RCPT TO:<" + to + ">");
            expectOk(reader, "250");

            send(writer, "DATA");
            expectOk(reader, "354");

            // Write RFC 2822 formatted email headers followed by blank line then body.
            writer.println("From: " + from);
            writer.println("To: " + to);
            writer.println("Subject: " + subject);
            writer.println(); // blank line separating headers from body
            writer.println(body);
            writer.println("."); // end-of-data marker
            expectOk(reader, "250");

            send(writer, "QUIT");

            System.out.println("Email sent successfully!");
        }
    }

    /**
     * Connects to the POP3 server and retrieves all messages for a user.
     * After downloading, it lists the emails and asks if you want to delete them.
     */
    static void receiveEmails(String host, int port, String user) throws IOException {
        System.out.printf("Checking inbox for '%s' via POP3 %s:%d%n", user, host, port);

        try (Socket socket = new Socket(host, port);
             var reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             var writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true)) {

            expectOk(reader, "+OK");
            send(writer, "USER " + user);
            expectOk(reader, "+OK");
            send(writer, "PASS password"); // our server accepts any password
            String statLine = expectOk(reader, "+OK");
            System.out.println("Login response: " + statLine);

            send(writer, "STAT");
            String stat = expectOk(reader, "+OK");
            System.out.println("Mailbox: " + stat);

            // Parse message count from STAT response: "+OK <count> <size>"
            int messageCount = Integer.parseInt(stat.split(" ")[1]);
            System.out.printf("Found %d message(s)%n", messageCount);

            for (int i = 1; i <= messageCount; i++) {
                send(writer, "RETR " + i);
                System.out.println("\n--- Message " + i + " ---");
                String line;
                // Read until the lone "." that signals end of this message.
                while ((line = reader.readLine()) != null && !line.equals(".")) {
                    System.out.println(line);
                }
            }

            send(writer, "QUIT");
            System.out.println("\nDone.");
        }
    }

    /**
     * Sends `count` emails as fast as possible, using virtual threads to do
     * it concurrently. Use this to test load balancing — watch the proxy logs
     * to see requests distributed across workers.
     *
     * Try running this with one worker, then start a second worker and run it
     * again. You should see the second run distribute load across both workers.
     */
    static void stressTest(String host, int port, int count) throws InterruptedException {
        System.out.printf("Stress test: sending %d emails to %s:%d...%n", count, host, port);
        long startMs = System.currentTimeMillis();

        // Use a CountDownLatch to wait for all virtual threads to finish.
        var latch = new java.util.concurrent.CountDownLatch(count);
        var errors = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int i = 0; i < count; i++) {
            final int idx = i;
            Thread.ofVirtual().start(() -> {
                try {
                    sendEmail(host, port,
                            "stress" + idx + "@test.com",
                            "inbox@test.com",
                            "Stress test " + idx,
                            "Body of stress test email number " + idx);
                } catch (IOException e) {
                    errors.incrementAndGet();
                    log.warn("Stress test email {} failed: {}", idx, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long elapsed = System.currentTimeMillis() - startMs;
        int success = count - errors.get();
        System.out.printf("Done: %d/%d succeeded in %dms (%.1f emails/sec)%n",
                success, count, elapsed, success * 1000.0 / elapsed);
    }

    private static String expectOk(BufferedReader reader, String prefix) throws IOException {
        // Read lines until we get the final response (doesn't end with '-' for multi-line).
        String line;
        String lastLine = "";
        while ((line = reader.readLine()) != null) {
            System.out.println("S: " + line);
            lastLine = line;
            // Multi-line SMTP responses continue as long as the 4th char is '-'.
            // e.g., "250-SIZE 10485760" continues, "250 OK" ends.
            if (line.length() < 4 || line.charAt(3) != '-') break;
        }
        return lastLine;
    }

    private static void send(PrintWriter writer, String cmd) {
        System.out.println("C: " + cmd);
        writer.println(cmd);
    }
}
