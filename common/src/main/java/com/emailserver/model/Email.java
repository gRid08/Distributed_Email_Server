package com.emailserver.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a single email message.
 *
 * This is an immutable value object — once created, it never changes.
 * Immutability is important in distributed systems because messages get
 * passed across thread boundaries, and mutable shared state is a source
 * of race conditions.
 *
 * We use Jackson annotations so this object can be serialized to JSON
 * for transmission over the wire between proxy and worker nodes.
 * (This is what protobuf did for you in gRPC — we're doing it manually.)
 */
public final class Email {

    // Every email gets a unique ID at creation time.
    // UUID.randomUUID() gives us a 128-bit globally unique identifier —
    // the probability of a collision is astronomically small.
    private final String id;

    private final String from;
    private final String to;
    private final String subject;
    private final String body;
    private final Instant timestamp;
    private final boolean deleted; // for POP3 DELE command (soft delete)

    // @JsonCreator tells Jackson to use this constructor when deserializing
    // from JSON. Without it, Jackson wouldn't know how to rebuild the object.
    @JsonCreator
    public Email(
            @JsonProperty("id")        String id,
            @JsonProperty("from")      String from,
            @JsonProperty("to")        String to,
            @JsonProperty("subject")   String subject,
            @JsonProperty("body")      String body,
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("deleted")   boolean deleted) {
        this.id        = id;
        this.from      = from;
        this.to        = to;
        this.subject   = subject;
        this.body      = body;
        this.timestamp = timestamp;
        this.deleted   = deleted;
    }

    // Factory method — creates a brand-new email with a generated ID and
    // current timestamp. This is the entry point for inbound SMTP messages.
    public static Email create(String from, String to, String subject, String body) {
        return new Email(
                UUID.randomUUID().toString(),
                from, to, subject, body,
                Instant.now(),
                false
        );
    }

    // Since this object is immutable, "deleting" means creating a new
    // instance with deleted=true. This is the functional programming pattern
    // called "copy-on-write" — safe across threads with no locks needed.
    public Email markDeleted() {
        return new Email(id, from, to, subject, body, timestamp, true);
    }

    public String getId()        { return id; }
    public String getFrom()      { return from; }
    public String getTo()        { return to; }
    public String getSubject()   { return subject; }
    public String getBody()      { return body; }
    public Instant getTimestamp(){ return timestamp; }
    public boolean isDeleted()   { return deleted; }

    // Returns the size in bytes — POP3's LIST command requires this.
    public int sizeInBytes() {
        return (from + to + subject + body).getBytes().length;
    }

    @Override
    public String toString() {
        return String.format("Email{id=%s, from=%s, to=%s, subject=%s}", id, from, to, subject);
    }
}
