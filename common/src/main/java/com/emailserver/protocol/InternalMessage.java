package com.emailserver.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Internal wire protocol between the proxy and worker nodes.
 *
 * In your gRPC version, protobuf handled this for you — you defined
 * message types in .proto files and the compiler generated serialization
 * code. Here we're doing the same thing manually with Jackson.
 *
 * Every message on the wire looks like:
 *   {"type":"STORE_EMAIL","payload":{...}}\n
 *
 * The \n (newline) acts as our message delimiter — the receiver reads
 * until it hits a newline and then knows it has a complete message.
 * This is called a "line-delimited protocol." Alternatives include
 * length-prefixing (sending the byte count first) or using a special
 * end-of-message marker. gRPC uses length-prefixing internally.
 *
 * @JsonTypeInfo + @JsonSubTypes is Jackson's polymorphism support —
 * it embeds a "type" discriminator field in the JSON so we know which
 * subclass to deserialize into when reading from the wire.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InternalMessage.StoreEmailRequest.class,  name = "STORE_EMAIL"),
        @JsonSubTypes.Type(value = InternalMessage.FetchEmailsRequest.class, name = "FETCH_EMAILS"),
        @JsonSubTypes.Type(value = InternalMessage.DeleteEmailRequest.class,  name = "DELETE_EMAIL"),
        @JsonSubTypes.Type(value = InternalMessage.HealthCheckRequest.class,  name = "HEALTH_CHECK"),
        @JsonSubTypes.Type(value = InternalMessage.RegisterRequest.class,     name = "REGISTER"),
        @JsonSubTypes.Type(value = InternalMessage.SuccessResponse.class,     name = "SUCCESS"),
        @JsonSubTypes.Type(value = InternalMessage.ErrorResponse.class,       name = "ERROR"),
        @JsonSubTypes.Type(value = InternalMessage.HealthCheckResponse.class, name = "HEALTH_RESPONSE"),
        @JsonSubTypes.Type(value = InternalMessage.FetchEmailsResponse.class, name = "FETCH_RESPONSE"),
})
public abstract sealed class InternalMessage
        // "sealed" is a Java 17+ feature that restricts which classes can extend this.
        // It's perfect here because we have a fixed, known set of message types —
        // the compiler can verify we've handled every case in switch statements.
        permits InternalMessage.StoreEmailRequest,
                InternalMessage.FetchEmailsRequest,
                InternalMessage.DeleteEmailRequest,
                InternalMessage.HealthCheckRequest,
                InternalMessage.RegisterRequest,
                InternalMessage.SuccessResponse,
                InternalMessage.ErrorResponse,
                InternalMessage.HealthCheckResponse,
                InternalMessage.FetchEmailsResponse {

    // ── Requests (proxy → worker) ──────────────────────────────────────────

    /** Proxy asks a worker to store an incoming email. */
    public static final class StoreEmailRequest extends InternalMessage {
        public final String from;
        public final String to;
        public final String subject;
        public final String body;

        @JsonCreator
        public StoreEmailRequest(
                @JsonProperty("from")    String from,
                @JsonProperty("to")      String to,
                @JsonProperty("subject") String subject,
                @JsonProperty("body")    String body) {
            this.from = from; this.to = to;
            this.subject = subject; this.body = body;
        }
    }

    /** Proxy asks a worker to retrieve all emails for a mailbox. */
    public static final class FetchEmailsRequest extends InternalMessage {
        public final String mailbox;

        @JsonCreator
        public FetchEmailsRequest(@JsonProperty("mailbox") String mailbox) {
            this.mailbox = mailbox;
        }
    }

    /** Proxy asks a worker to delete (mark deleted) a specific email. */
    public static final class DeleteEmailRequest extends InternalMessage {
        public final String mailbox;
        public final String emailId;

        @JsonCreator
        public DeleteEmailRequest(
                @JsonProperty("mailbox") String mailbox,
                @JsonProperty("emailId") String emailId) {
            this.mailbox = mailbox;
            this.emailId = emailId;
        }
    }

    /**
     * Proxy pings a worker to verify it's alive.
     * This is the heartbeat your health check system sends.
     * The worker responds with a HealthCheckResponse containing
     * its current metrics — so the health check also doubles as
     * a load reporting mechanism. One round-trip, two purposes.
     */
    public static final class HealthCheckRequest extends InternalMessage {
        // Empty body — just sending this message IS the ping.
        @JsonCreator
        public HealthCheckRequest() {}
    }

    /**
     * A new worker node announces itself to the proxy.
     * This is the "push registration" model — the worker knows
     * the proxy's control port and calls it on startup.
     */
    public static final class RegisterRequest extends InternalMessage {
        public final String workerId;
        public final String host;
        public final int smtpPort;
        public final int pop3Port;
        public final int controlPort;

        @JsonCreator
        public RegisterRequest(
                @JsonProperty("workerId")     String workerId,
                @JsonProperty("host")         String host,
                @JsonProperty("smtpPort")     int smtpPort,
                @JsonProperty("pop3Port")     int pop3Port,
                @JsonProperty("controlPort")  int controlPort) {
            this.workerId    = workerId;
            this.host        = host;
            this.smtpPort    = smtpPort;
            this.pop3Port    = pop3Port;
            this.controlPort = controlPort;
        }
    }

    // ── Responses (worker → proxy) ─────────────────────────────────────────

    public static final class SuccessResponse extends InternalMessage {
        public final String message;

        @JsonCreator
        public SuccessResponse(@JsonProperty("message") String message) {
            this.message = message;
        }
    }

    public static final class ErrorResponse extends InternalMessage {
        public final String error;

        @JsonCreator
        public ErrorResponse(@JsonProperty("error") String error) {
            this.error = error;
        }
    }

    /**
     * Worker responds to a health check with its current vitals.
     * The proxy uses activeConnections and responseTimeMs to update
     * the worker's load score for adaptive routing decisions.
     */
    public static final class HealthCheckResponse extends InternalMessage {
        public final boolean healthy;
        public final int activeConnections;
        public final long responseTimeMs;
        public final long totalEmailsStored;

        @JsonCreator
        public HealthCheckResponse(
                @JsonProperty("healthy")           boolean healthy,
                @JsonProperty("activeConnections") int activeConnections,
                @JsonProperty("responseTimeMs")    long responseTimeMs,
                @JsonProperty("totalEmailsStored") long totalEmailsStored) {
            this.healthy            = healthy;
            this.activeConnections  = activeConnections;
            this.responseTimeMs     = responseTimeMs;
            this.totalEmailsStored  = totalEmailsStored;
        }
    }

    public static final class FetchEmailsResponse extends InternalMessage {
        public final java.util.List<com.emailserver.model.Email> emails;

        @JsonCreator
        public FetchEmailsResponse(
                @JsonProperty("emails") java.util.List<com.emailserver.model.Email> emails) {
            this.emails = emails;
        }
    }
}
