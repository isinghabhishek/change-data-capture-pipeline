package cdc.schema;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Utility class for determining whether an Iceberg column type change is a
 * safe (lossless) widening.
 *
 * <h2>Widening rules (design §6.6 / Requirements 7.3, 7.4)</h2>
 * <ul>
 *   <li>{@code INT → LONG} — 32-bit integer widened to 64-bit</li>
 *   <li>{@code FLOAT → DOUBLE} — 32-bit float widened to 64-bit</li>
 *   <li>{@code DECIMAL(p, s) → DECIMAL(p', s)} where {@code p' > p} and
 *       the scale is unchanged — more precision digits, same fractional digits</li>
 * </ul>
 *
 * <p>All other type changes — narrowing, cross-family, scale changes, or
 * changes involving complex types — return {@code false}.
 */
public final class TypeCompatibility {

    private TypeCompatibility() {
        // utility class — no instances
    }

    /**
     * Returns {@code true} if changing an Iceberg column from {@code fromType}
     * to {@code toType} is a safe, lossless widening.
     *
     * <p>This method is intentionally pure (no I/O, no state) so that it can
     * be tested exhaustively with property-based tests.
     *
     * @param fromType the current (original) column type
     * @param toType   the incoming (new) column type
     * @return {@code true} iff the change is a widening as defined above
     * @throws IllegalArgumentException if either argument is {@code null}
     */
    public static boolean isTypeWidening(Type fromType, Type toType) {
        if (fromType == null || toType == null) {
            throw new IllegalArgumentException(
                    "fromType and toType must not be null");
        }

        // Fast path: identical types are not a widening (no change needed).
        if (fromType.equals(toType)) {
            return false;
        }

        Type.TypeID fromId = fromType.typeId();
        Type.TypeID toId   = toType.typeId();

        // INT → LONG
        if (fromId == Type.TypeID.INTEGER && toId == Type.TypeID.LONG) {
            return true;
        }

        // FLOAT → DOUBLE
        if (fromId == Type.TypeID.FLOAT && toId == Type.TypeID.DOUBLE) {
            return true;
        }

        // DECIMAL(p, s) → DECIMAL(p', s) where p' > p (scale must match)
        if (fromId == Type.TypeID.DECIMAL && toId == Type.TypeID.DECIMAL) {
            Types.DecimalType fromDecimal = (Types.DecimalType) fromType;
            Types.DecimalType toDecimal   = (Types.DecimalType) toType;
            return toDecimal.precision() > fromDecimal.precision()
                    && toDecimal.scale() == fromDecimal.scale();
        }

        return false;
    }
}
