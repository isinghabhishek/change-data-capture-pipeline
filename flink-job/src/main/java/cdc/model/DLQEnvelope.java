package cdc.model;

import java.io.Serializable;

/**
 * Envelope for messages written to the Dead Letter Queue topic
 * ({@code cdc.dead-letter-queue}).
 *
 * <p>Schema is defined in design §3.3 and reproduced here as a plain Java POJO
 * for use with Flink's serialisation framework.  All fields must be populated
 * before a message is published to the DLQ (see Property 10 / Requirement 8.2).
 */
public class DLQEnvelope implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Kafka topic the original message was consumed from. */
    public String original_topic;

    /** Kafka partition number the original message was consumed from. */
    public int original_partition;

    /** Kafka offset of the original message. */
    public long original_offset;

    /** Classification of the error that caused the message to be dead-lettered. */
    public ErrorType error_type;

    /** Human-readable description of the error. */
    public String error_message;

    /**
     * UTC timestamp (milliseconds since epoch) at which the error was detected.
     * Corresponds to the Avro logical type {@code timestamp-millis}.
     */
    public long error_timestamp_utc;

    /**
     * Raw bytes of the original Kafka message value.
     * Preserved verbatim so that the replay utility can re-publish the exact payload.
     */
    public byte[] raw_payload;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** No-arg constructor required by Flink POJO serialisation. */
    public DLQEnvelope() {}

    /** All-args constructor for convenience (e.g. in {@code DLQRouter}). */
    public DLQEnvelope(String original_topic, int original_partition, long original_offset,
                       ErrorType error_type, String error_message,
                       long error_timestamp_utc, byte[] raw_payload) {
        this.original_topic     = original_topic;
        this.original_partition = original_partition;
        this.original_offset    = original_offset;
        this.error_type         = error_type;
        this.error_message      = error_message;
        this.error_timestamp_utc = error_timestamp_utc;
        this.raw_payload        = raw_payload;
    }

    @Override
    public String toString() {
        return "DLQEnvelope{" +
               "original_topic='" + original_topic + '\'' +
               ", original_partition=" + original_partition +
               ", original_offset=" + original_offset +
               ", error_type=" + error_type +
               ", error_message='" + error_message + '\'' +
               ", error_timestamp_utc=" + error_timestamp_utc +
               ", raw_payload.length=" + (raw_payload != null ? raw_payload.length : "null") +
               '}';
    }
}
