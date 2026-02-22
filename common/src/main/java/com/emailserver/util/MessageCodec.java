package com.emailserver.util;

import com.emailserver.protocol.InternalMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Handles serialization and deserialization of InternalMessage objects.
 *
 * This is the "codec" — it sits at the boundary between the Java object world
 * and the wire (bytes flowing through the TCP socket). Every message must pass
 * through this layer twice: once when sending (object → JSON string → bytes)
 * and once when receiving (bytes → JSON string → object).
 *
 * We use a single shared ObjectMapper instance. ObjectMapper is thread-safe
 * after configuration, so sharing one instance is both safe and more efficient
 * than creating a new one per message (ObjectMapper creation is expensive).
 *
 * Why JSON instead of something like protobuf or Avro?
 *   - Human readable: you can log a message and immediately understand it.
 *   - No schema compilation step (protobuf requires running protoc).
 *   - Flexible: easy to add fields without breaking existing nodes.
 *   The trade-off is slightly larger messages and slower parsing — acceptable
 *   for a learning system, and often acceptable in production too.
 */
public final class MessageCodec {

    // Static singleton — configured once, used everywhere.
    private static final ObjectMapper MAPPER = buildMapper();

    private static ObjectMapper buildMapper() {
        ObjectMapper m = new ObjectMapper();
        // JavaTimeModule teaches Jackson how to handle java.time.Instant
        // (it's not supported by default because it was added in Java 8
        // after Jackson was originally designed).
        m.registerModule(new JavaTimeModule());
        // By default Jackson would serialize Instant as a floating-point
        // Unix timestamp. WRITE_DATES_AS_TIMESTAMPS(false) makes it use
        // ISO-8601 string format instead — much more readable in logs.
        m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return m;
    }

    private MessageCodec() {} // utility class — no instances needed

    /**
     * Serialize a message to a JSON string with a trailing newline.
     * The newline is our message delimiter — the receiving side reads
     * line-by-line and knows each line is exactly one complete message.
     *
     * This is the framing strategy. Without framing, TCP's stream nature
     * means you might receive half a message, or two messages concatenated
     * together, and you'd have no way to tell where one ends and the next begins.
     */
    public static String encode(InternalMessage message) {
        try {
            return MAPPER.writeValueAsString(message) + "\n";
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode message: " + message, e);
        }
    }

    /**
     * Deserialize a JSON string back into an InternalMessage.
     * Jackson uses the "type" field (set by @JsonTypeInfo) to determine
     * which concrete subclass to instantiate.
     */
    public static InternalMessage decode(String json) {
        try {
            return MAPPER.readValue(json.trim(), InternalMessage.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to decode message: " + json, e);
        }
    }

    // Expose mapper for cases where fine-grained JSON control is needed
    public static ObjectMapper getMapper() { return MAPPER; }
}
