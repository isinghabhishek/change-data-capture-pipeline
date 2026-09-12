package cdc.operator;

import cdc.model.ChangeEvent;
import cdc.model.ChangeEvent.TransactionBlock;
import cdc.model.DLQEnvelope;
import cdc.model.ErrorType;
import cdc.model.SourceInfo;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Avro deserializer for Debezium Change Events consumed from Kafka.
 *
 * <h2>Wire format (Confluent Schema Registry framing)</h2>
 * <pre>
 * Byte 0      – magic byte (0x00)
 * Bytes 1–4   – schema ID (big-endian signed int)
 * Bytes 5+    – Avro binary payload
 * </pre>
 *
 * <h2>Error handling</h2>
 * If deserialization fails for any reason the raw bytes together with Kafka
 * record metadata (topic, partition, offset) are packaged into a
 * {@link DLQEnvelope} with {@code error_type = DESERIALIZATION_FAILURE} and
 * emitted to the {@link #DLQ_OUTPUT_TAG} side output.  The main output receives
 * no element for that record and processing continues with the next record.
 *
 * <h2>Side-output wiring</h2>
 * This class implements {@link KafkaRecordDeserializationSchema} so that the
 * full {@link ConsumerRecord} metadata (topic/partition/offset) is accessible
 * at decode time.  Because {@link KafkaRecordDeserializationSchema#deserialize}
 * does not expose a Flink {@code ProcessFunction.Context}, DLQ messages are
 * returned to the caller via {@link #takePendingDlqEnvelope()}.  The wrapping
 * Flink operator (see {@code CdcPipelineJob}) calls this method after each
 * deserialization and routes the envelope to {@link #DLQ_OUTPUT_TAG} via
 * {@code ctx.output(dlqOutputTag, envelope)} when non-{@code null}.
 */
public class AvroDeserializer
        implements KafkaRecordDeserializationSchema<ChangeEvent>, Serializable {

    private static final long serialVersionUID = 1L;

    /** Magic byte that prefixes every Confluent Schema Registry Avro message. */
    private static final byte MAGIC_BYTE = 0x00;

    /** Maximum number of schemas cached locally per Schema Registry client. */
    private static final int SCHEMA_CACHE_CAPACITY = 1000;

    // -------------------------------------------------------------------------
    // Public constants
    // -------------------------------------------------------------------------

    /**
     * Side-output tag for {@link DLQEnvelope} messages arising from
     * deserialization failures.  Reference this tag in the job wiring to
     * attach a Kafka DLQ sink to the side output.
     */
    public static final OutputTag<DLQEnvelope> DLQ_OUTPUT_TAG =
            new OutputTag<DLQEnvelope>("dlq-deserialization-failures") {};

    // -------------------------------------------------------------------------
    // Configuration (serializable — sent with the operator snapshot)
    // -------------------------------------------------------------------------

    private final String schemaRegistryUrl;
    private final OutputTag<DLQEnvelope> dlqOutputTag;

    // -------------------------------------------------------------------------
    // Transient runtime state (re-created per task-manager JVM)
    // -------------------------------------------------------------------------

    /**
     * Confluent Schema Registry client; lazily initialised on first use.
     * Not serialised — re-created after task-manager restart.
     */
    private transient SchemaRegistryClient schemaRegistryClient;

    /**
     * Holds the most recently generated DLQ envelope so the wrapping operator
     * can retrieve and forward it to the side output.  Cleared by
     * {@link #takePendingDlqEnvelope()} after each call to
     * {@link #deserialize}.
     */
    private transient DLQEnvelope pendingDlqEnvelope;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a new {@code AvroDeserializer}.
     *
     * @param schemaRegistryUrl base URL of the Confluent Schema Registry
     *                          (e.g. {@code "http://schema-registry:8081"})
     * @param dlqOutputTag      the side-output tag to which
     *                          {@link DLQEnvelope} messages should be routed
     *                          by the wrapping operator
     */
    public AvroDeserializer(String schemaRegistryUrl,
                            OutputTag<DLQEnvelope> dlqOutputTag) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.dlqOutputTag      = dlqOutputTag;
    }

    // -------------------------------------------------------------------------
    // KafkaRecordDeserializationSchema
    // -------------------------------------------------------------------------

    /**
     * Deserializes a raw Kafka {@link ConsumerRecord} into a
     * {@link ChangeEvent}.
     *
     * <p>On success: {@code out.collect(event)} is called and
     * {@link #takePendingDlqEnvelope()} returns {@code null}.
     *
     * <p>On failure: a {@link DLQEnvelope} is stored in the pending field
     * (retrievable via {@link #takePendingDlqEnvelope()}), no element is
     * emitted to {@code out}, and the method returns normally so that the
     * next record can be processed.
     *
     * @param record the raw Kafka consumer record
     * @param out    collector for successfully deserialized {@link ChangeEvent}s
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<ChangeEvent> out) throws IOException {
        // Clear any leftover envelope from the previous call
        pendingDlqEnvelope = null;

        byte[] rawBytes = record.value();
        try {
            ChangeEvent event = decodeAvro(rawBytes);
            out.collect(event);
        } catch (Exception e) {
            on_error(
                record.topic(),
                record.partition(),
                record.offset(),
                e,
                rawBytes
            );
            // Do NOT rethrow — processing continues with the next record.
        }
    }

    // -------------------------------------------------------------------------
    // DeserializationSchema lifecycle / type
    // -------------------------------------------------------------------------

    /**
     * Returns {@link TypeInformation} for {@link ChangeEvent} so that Flink
     * can choose the correct serializer for checkpointing and network transfer.
     * <p>
     * Note: {@code KafkaRecordDeserializationSchema} does not declare
     * {@code getProducedType()} as an abstract method in Flink 1.18, but
     * providing it makes the produced type explicit and avoids generic-type
     * erasure issues during job submission.
     */
    @Override
    public TypeInformation<ChangeEvent> getProducedType() {
        return TypeInformation.of(ChangeEvent.class);
    }

    // -------------------------------------------------------------------------
    // DLQ envelope hand-off to wrapping operator
    // -------------------------------------------------------------------------

    /**
     * Returns and clears the pending {@link DLQEnvelope} produced during the
     * last call to {@link #deserialize}.
     *
     * <p>The wrapping Flink operator calls this after each invocation of
     * {@link #deserialize}.  If the return value is non-{@code null} the
     * operator emits the envelope to the DLQ side output:
     * <pre>{@code
     *   DLQEnvelope dlq = deserializer.takePendingDlqEnvelope();
     *   if (dlq != null) {
     *       ctx.output(AvroDeserializer.DLQ_OUTPUT_TAG, dlq);
     *   }
     * }</pre>
     *
     * @return the pending envelope, or {@code null} if the last record was
     *         deserialized successfully
     */
    public DLQEnvelope takePendingDlqEnvelope() {
        DLQEnvelope envelope = pendingDlqEnvelope;
        pendingDlqEnvelope = null;
        return envelope;
    }

    // -------------------------------------------------------------------------
    // Error routing
    // -------------------------------------------------------------------------

    /**
     * Builds a {@link DLQEnvelope} and stores it as the pending envelope so
     * it can be forwarded to {@link #DLQ_OUTPUT_TAG} by the wrapping operator
     * via {@code ctx.output()}.  This method does not rethrow the exception;
     * processing continues with the next event.
     *
     * <p>All seven required DLQ fields are populated:
     * <ul>
     *   <li>{@code original_topic} — Kafka topic name</li>
     *   <li>{@code original_partition} — Kafka partition index</li>
     *   <li>{@code original_offset} — Kafka message offset</li>
     *   <li>{@code error_type} — always {@link ErrorType#DESERIALIZATION_FAILURE}</li>
     *   <li>{@code error_message} — exception message (or "unknown error")</li>
     *   <li>{@code error_timestamp_utc} — current wall-clock time in millis</li>
     *   <li>{@code raw_payload} — original raw Kafka value bytes</li>
     * </ul>
     *
     * @param topic      Kafka topic the record was consumed from
     * @param partition  Kafka partition index
     * @param offset     Kafka offset of the failed record
     * @param cause      the exception that caused the failure
     * @param rawPayload the original raw Kafka record value bytes
     */
    private void on_error(String topic,
                          int partition,
                          long offset,
                          Exception cause,
                          byte[] rawPayload) {
        DLQEnvelope envelope = new DLQEnvelope();
        envelope.original_topic      = topic;
        envelope.original_partition  = partition;
        envelope.original_offset     = offset;
        envelope.error_type          = ErrorType.DESERIALIZATION_FAILURE;
        envelope.error_message       = (cause != null && cause.getMessage() != null)
                                       ? cause.getMessage()
                                       : "unknown deserialization error";
        envelope.error_timestamp_utc = System.currentTimeMillis();
        envelope.raw_payload         = (rawPayload != null) ? rawPayload : new byte[0];

        this.pendingDlqEnvelope = envelope;
    }

    // -------------------------------------------------------------------------
    // Avro decoding
    // -------------------------------------------------------------------------

    /**
     * Decodes Avro-encoded bytes using the Confluent Schema Registry wire
     * format and converts the resulting {@link GenericRecord} to a
     * {@link ChangeEvent}.
     *
     * @param bytes the raw value bytes (must contain magic byte + 4-byte schema
     *              ID + Avro binary payload)
     * @return a fully populated {@link ChangeEvent}
     * @throws IllegalArgumentException if the payload is too short or has an
     *                                  invalid magic byte
     * @throws IOException              on Avro I/O errors
     * @throws RestClientException      if the Schema Registry cannot resolve the
     *                                  schema ID
     */
    private ChangeEvent decodeAvro(byte[] bytes)
            throws IOException, RestClientException {

        if (bytes == null || bytes.length < 5) {
            throw new IllegalArgumentException(
                    "Avro payload too short — expected at least 5 bytes "
                    + "(magic byte + 4-byte schema ID), got: "
                    + (bytes == null ? "null" : bytes.length));
        }

        if (bytes[0] != MAGIC_BYTE) {
            throw new IllegalArgumentException(
                    "Invalid magic byte: expected 0x00, got 0x"
                    + Integer.toHexString(bytes[0] & 0xFF));
        }

        // Extract schema ID from bytes 1–4 (big-endian)
        int schemaId = ByteBuffer.wrap(bytes, 1, 4).getInt();

        // Resolve the writer schema from the Schema Registry (cached)
        Schema schema = resolveSchema(schemaId);

        // Decode the Avro binary payload starting at byte offset 5
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(bytes, 5, bytes.length - 5, null);
        GenericRecord record = reader.read(null, decoder);

        return toChangeEvent(record);
    }

    /**
     * Looks up the Avro {@link Schema} for the given {@code schemaId} in the
     * Schema Registry, using the local cache where possible.
     */
    private Schema resolveSchema(int schemaId)
            throws IOException, RestClientException {
        SchemaRegistryClient client = getOrCreateClient();
        io.confluent.kafka.schemaregistry.avro.AvroSchema avroSchema =
                (io.confluent.kafka.schemaregistry.avro.AvroSchema)
                        client.getSchemaById(schemaId);
        return avroSchema.rawSchema();
    }

    /**
     * Returns the {@link SchemaRegistryClient}, creating it lazily on first
     * call.  The client maintains an in-process LRU cache of up to
     * {@link #SCHEMA_CACHE_CAPACITY} schemas.
     */
    private SchemaRegistryClient getOrCreateClient() {
        if (schemaRegistryClient == null) {
            schemaRegistryClient = new CachedSchemaRegistryClient(
                    schemaRegistryUrl, SCHEMA_CACHE_CAPACITY);
        }
        return schemaRegistryClient;
    }

    // -------------------------------------------------------------------------
    // GenericRecord → domain model mapping
    // -------------------------------------------------------------------------

    /**
     * Converts a Debezium Avro envelope {@link GenericRecord} to a
     * {@link ChangeEvent}.
     *
     * <p>Field mapping:
     * <ul>
     *   <li>{@code before}/{@code after} — nullable; converted to
     *       {@link Map}{@code <String, Object>} via {@link #recordToMap}</li>
     *   <li>{@code source} — mapped to {@link SourceInfo}</li>
     *   <li>{@code op}     — converted to {@link String}</li>
     *   <li>{@code ts_ms}  — {@link Long}, defaults to {@code 0} if null</li>
     *   <li>{@code transaction} — nullable; mapped to {@link TransactionBlock}</li>
     * </ul>
     */
    private ChangeEvent toChangeEvent(GenericRecord envelope) {
        ChangeEvent event = new ChangeEvent();

        Object beforeRaw = envelope.get("before");
        Object afterRaw  = envelope.get("after");
        event.before = (beforeRaw instanceof GenericRecord)
                ? recordToMap((GenericRecord) beforeRaw) : null;
        event.after  = (afterRaw instanceof GenericRecord)
                ? recordToMap((GenericRecord) afterRaw) : null;

        GenericRecord sourceRecord = (GenericRecord) envelope.get("source");
        event.source = toSourceInfo(sourceRecord);

        Object opObj = envelope.get("op");
        event.op = (opObj != null) ? opObj.toString() : null;

        Object tsMs = envelope.get("ts_ms");
        event.ts_ms = (tsMs instanceof Long) ? (Long) tsMs : 0L;

        Object txRaw = envelope.get("transaction");
        event.transaction = (txRaw instanceof GenericRecord)
                ? toTransactionBlock((GenericRecord) txRaw) : null;

        return event;
    }

    /**
     * Maps a Debezium {@code source} sub-record to a {@link SourceInfo}.
     */
    private SourceInfo toSourceInfo(GenericRecord source) {
        if (source == null) {
            return null;
        }
        SourceInfo info = new SourceInfo();
        info.version   = getString(source, "version");
        info.connector = getString(source, "connector");
        info.name      = getString(source, "name");
        info.ts_ms     = getLong(source, "ts_ms", 0L);
        info.db        = getString(source, "db");
        info.schema    = getString(source, "schema");
        info.table     = getString(source, "table");
        info.lsn       = getNullableLong(source, "lsn");
        info.txId      = getNullableLong(source, "txId");
        return info;
    }

    /**
     * Maps a Debezium optional {@code transaction} sub-record to a
     * {@link TransactionBlock}.
     */
    private TransactionBlock toTransactionBlock(GenericRecord tx) {
        TransactionBlock block = new TransactionBlock();
        block.id                    = getString(tx, "id");
        block.total_order            = getLong(tx, "total_order", 0L);
        block.data_collection_order  = getLong(tx, "data_collection_order", 0L);
        return block;
    }

    /**
     * Converts a row-image {@link GenericRecord} to a
     * {@link Map}{@code <String, Object>} by iterating its schema fields.
     *
     * <p>Avro {@link org.apache.avro.util.Utf8} string values are converted to
     * {@link String} to avoid serialization issues downstream.
     */
    private Map<String, Object> recordToMap(GenericRecord record) {
        Map<String, Object> map = new HashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            if (value instanceof org.apache.avro.util.Utf8) {
                value = value.toString();
            }
            map.put(field.name(), value);
        }
        return map;
    }

    // -------------------------------------------------------------------------
    // Avro field extraction helpers
    // -------------------------------------------------------------------------

    private static String getString(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        return (value != null) ? value.toString() : null;
    }

    private static long getLong(GenericRecord record, String fieldName,
                                long defaultValue) {
        Object value = record.get(fieldName);
        return (value instanceof Long) ? (Long) value : defaultValue;
    }

    private static Long getNullableLong(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        return (value instanceof Long) ? (Long) value : null;
    }
}
