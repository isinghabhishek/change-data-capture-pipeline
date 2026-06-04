package cdc.model;

/**
 * Classifies why a Change Event was routed to the Dead Letter Queue.
 *
 * <p>Maps directly to the {@code ErrorType} enum in the DLQ Avro schema (design §3.3)
 * and to the error classification table in design §5.4.
 */
public enum ErrorType {

    /** Avro bytes could not be decoded (corrupt bytes or unknown schema ID). */
    DESERIALIZATION_FAILURE,

    /** Schema Registry rejected a publish due to a breaking schema change. */
    SCHEMA_INCOMPATIBLE,

    /** A column type change is not backward-compatible (e.g. VARCHAR → INT). */
    TYPE_MISMATCH,

    /** Any other Flink processing exception not covered by the above categories. */
    PROCESSING_ERROR
}
