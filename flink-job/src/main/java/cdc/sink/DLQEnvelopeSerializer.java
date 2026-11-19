package cdc.sink;

import cdc.model.DLQEnvelope;

import org.apache.flink.api.common.serialization.SerializationSchema;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * Serializes a {@link DLQEnvelope} to JSON bytes for publication to the
 * Kafka DLQ topic ({@code cdc.dead-letter-queue}).
 *
 * <p>Uses Jackson for JSON serialisation.  The raw_payload byte array is
 * base64-encoded automatically by Jackson's default {@code byte[]} handler.
 *
 * <p>This class is used as the value serialiser in the DLQ Kafka producer
 * sink wired in {@link cdc.CdcPipelineJob}.
 */
public class DLQEnvelopeSerializer
        implements SerializationSchema<DLQEnvelope>, Serializable {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(DLQEnvelope envelope) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            return mapper.writeValueAsBytes(envelope);
        } catch (Exception e) {
            // Serialisation of the DLQ envelope itself failed — return a
            // minimal fallback payload so the message is never silently lost.
            String fallback = "{\"error\":\"DLQEnvelopeSerializer failed\","
                    + "\"cause\":\"" + e.getMessage() + "\","
                    + "\"error_type\":\"" + (envelope != null ? envelope.error_type : "UNKNOWN") + "\"}";
            return fallback.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
