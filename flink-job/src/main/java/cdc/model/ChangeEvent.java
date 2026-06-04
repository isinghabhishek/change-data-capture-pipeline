package cdc.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Java POJO representing a Debezium Change Event envelope (design §3.1).
 *
 * <p>{@code before} and {@code after} are modelled as {@code Map<String, Object>} so that
 * dynamic Avro Value records with per-table field sets can be represented without
 * generating table-specific classes.
 *
 * <p>This class follows Flink POJO conventions:
 * <ul>
 *   <li>Public fields (no private + getters required for Flink's reflection-based serialiser)</li>
 *   <li>No-arg constructor</li>
 *   <li>Implements {@link Serializable}</li>
 * </ul>
 */
public class ChangeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Row image before the change (null for INSERT / snapshot reads).
     * Keys are column names; values are the column values decoded from Avro.
     */
    public Map<String, Object> before;

    /**
     * Row image after the change (null for DELETE).
     * Keys are column names; values are the column values decoded from Avro.
     */
    public Map<String, Object> after;

    /** Debezium source metadata (connector, table, WAL position, etc.). */
    public SourceInfo source;

    /**
     * Operation type as a single character string:
     * <ul>
     *   <li>{@code c} — INSERT (create)</li>
     *   <li>{@code u} — UPDATE</li>
     *   <li>{@code d} — DELETE</li>
     *   <li>{@code r} — READ (snapshot)</li>
     *   <li>{@code t} — TRUNCATE</li>
     * </ul>
     */
    public String op;

    /** Debezium processing timestamp in milliseconds since epoch (nullable). */
    public Long ts_ms;

    /**
     * Transaction metadata block (nullable — only present when
     * {@code provide.transaction.metadata=true} is set on the connector).
     */
    public TransactionBlock transaction;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** No-arg constructor required by Flink POJO serialisation. */
    public ChangeEvent() {}

    /** All-args constructor for convenience (e.g. in tests). */
    public ChangeEvent(Map<String, Object> before, Map<String, Object> after,
                       SourceInfo source, String op, Long ts_ms,
                       TransactionBlock transaction) {
        this.before      = before;
        this.after       = after;
        this.source      = source;
        this.op          = op;
        this.ts_ms       = ts_ms;
        this.transaction = transaction;
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    /**
     * Derives the {@link PrimaryKey} for this event.
     *
     * <p>The PK value is taken from the {@code after} image when available
     * (INSERT / UPDATE / snapshot), and from {@code before} for DELETE events.
     * The key column name defaults to {@code "id"} — callers that know the real
     * PK column name should use {@link #primaryKey(String)} instead.
     *
     * @return a {@link PrimaryKey} wrapping the PK value
     * @throws IllegalStateException if neither before nor after contains a value
     */
    public PrimaryKey primaryKey() {
        return primaryKey("id");
    }

    /**
     * Derives the {@link PrimaryKey} for this event using the supplied column name.
     *
     * @param pkColumn the primary key column name
     * @return a {@link PrimaryKey} wrapping the PK value
     */
    public PrimaryKey primaryKey(String pkColumn) {
        Map<String, Object> image = (after != null) ? after : before;
        if (image == null) {
            throw new IllegalStateException(
                "ChangeEvent has neither before nor after image — cannot derive PrimaryKey");
        }
        return new PrimaryKey(image.get(pkColumn));
    }

    @Override
    public String toString() {
        return "ChangeEvent{" +
               "op='" + op + '\'' +
               ", ts_ms=" + ts_ms +
               ", source=" + source +
               ", before=" + before +
               ", after=" + after +
               ", transaction=" + transaction +
               '}';
    }

    // -------------------------------------------------------------------------
    // Inner class: TransactionBlock
    // -------------------------------------------------------------------------

    /**
     * Optional transaction metadata block embedded within a Change Event.
     * Present only when the Debezium connector is configured with
     * {@code provide.transaction.metadata=true}.
     */
    public static class TransactionBlock implements Serializable {

        private static final long serialVersionUID = 1L;

        /** Unique transaction identifier (string representation of the XID). */
        public String id;

        /** Total event order within the transaction across all tables. */
        public long total_order;

        /** Event order within the transaction for this specific data collection. */
        public long data_collection_order;

        /** No-arg constructor required by Flink POJO serialisation. */
        public TransactionBlock() {}

        /** All-args constructor for convenience. */
        public TransactionBlock(String id, long total_order, long data_collection_order) {
            this.id                    = id;
            this.total_order           = total_order;
            this.data_collection_order = data_collection_order;
        }

        @Override
        public String toString() {
            return "TransactionBlock{" +
                   "id='" + id + '\'' +
                   ", total_order=" + total_order +
                   ", data_collection_order=" + data_collection_order +
                   '}';
        }
    }
}
