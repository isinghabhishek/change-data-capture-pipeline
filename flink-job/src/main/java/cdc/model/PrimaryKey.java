package cdc.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Wrapper that represents the primary key value of a source row.
 *
 * <p>Used as the key type for Flink's {@code KeyedStream} in the
 * {@code DeduplicationOperator} (design §2.5 / §6.6).  Correct
 * {@link #equals(Object)} and {@link #hashCode()} implementations are
 * essential so that Flink routes all events for the same row to the same
 * parallel sub-task and so that state look-ups work correctly.
 *
 * <p>The wrapped {@code value} can be any scalar that the source table uses
 * as a primary key (Integer, Long, String, UUID string, etc.).  Composite
 * primary keys should be represented as a delimited string or a structured
 * object whose own {@code equals}/{@code hashCode} are correct.
 */
public class PrimaryKey implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The actual primary key value (nullable, though a null PK is unusual). */
    public Object value;

    /** No-arg constructor required by Flink POJO serialisation. */
    public PrimaryKey() {}

    /**
     * Constructs a {@code PrimaryKey} wrapping the given value.
     *
     * @param value the primary key value; may be {@code null}
     */
    public PrimaryKey(Object value) {
        this.value = value;
    }

    // -------------------------------------------------------------------------
    // equals / hashCode — critical for KeyedStream correctness
    // -------------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PrimaryKey)) {
            return false;
        }
        PrimaryKey other = (PrimaryKey) obj;
        return Objects.equals(this.value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return "PrimaryKey{value=" + value + '}';
    }
}
