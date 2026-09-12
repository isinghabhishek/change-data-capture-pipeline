package cdc.schema;

import org.apache.iceberg.types.Type;

/**
 * Thrown when a column type change detected in an incoming {@code ChangeEvent}
 * schema is not a safe widening and therefore cannot be applied to the target
 * Iceberg table automatically.
 *
 * <p>The {@link cdc.operator.SchemaEvolutionOperator} catches this exception
 * and routes the offending event to the Dead Letter Queue with
 * {@code error_type = TYPE_MISMATCH} (design §2.7, §5.2 / Requirement 7.4).
 */
public class IncompatibleTypeChangeException extends Exception {

    private static final long serialVersionUID = 1L;

    /** The name of the column whose type changed incompatibly. */
    private final String columnName;

    /** The current Iceberg type for the column. */
    private final Type currentType;

    /** The incoming (incompatible) type for the column. */
    private final Type incomingType;

    /**
     * Constructs a new {@code IncompatibleTypeChangeException}.
     *
     * @param columnName   the column whose type is changing
     * @param currentType  the current Iceberg column type
     * @param incomingType the incoming incompatible type
     */
    public IncompatibleTypeChangeException(String columnName,
                                           Type currentType,
                                           Type incomingType) {
        super("Incompatible type change for column '" + columnName
                + "': " + currentType + " → " + incomingType
                + " is not a safe widening");
        this.columnName   = columnName;
        this.currentType  = currentType;
        this.incomingType = incomingType;
    }

    /** @return the column name where the incompatible type change was detected */
    public String getColumnName() {
        return columnName;
    }

    /** @return the current (original) Iceberg type */
    public Type getCurrentType() {
        return currentType;
    }

    /** @return the incoming (incompatible) type */
    public Type getIncomingType() {
        return incomingType;
    }
}
