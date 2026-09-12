package cdc.schema;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and property-based tests for {@link TypeCompatibility#isTypeWidening}.
 *
 * <h2>Coverage</h2>
 * <ul>
 *   <li>All valid widening pairs return {@code true}</li>
 *   <li>Narrowing and incompatible pairs return {@code false}</li>
 *   <li>Identical types return {@code false} (no change needed)</li>
 *   <li>DECIMAL precision/scale edge cases</li>
 *   <li>Null argument guard</li>
 * </ul>
 *
 * <p>Requirements: 7.3, 7.4
 */
class TypeCompatibilityTest {

    // =========================================================================
    // Valid widening pairs — must return true
    // =========================================================================

    @Example
    void intToLong_isWidening() {
        assertTrue(TypeCompatibility.isTypeWidening(
                Types.IntegerType.get(), Types.LongType.get()));
    }

    @Example
    void floatToDouble_isWidening() {
        assertTrue(TypeCompatibility.isTypeWidening(
                Types.FloatType.get(), Types.DoubleType.get()));
    }

    @Example
    void decimal_higherPrecisionSameScale_isWidening() {
        // DECIMAL(10,2) → DECIMAL(12,2)
        assertTrue(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(10, 2), Types.DecimalType.of(12, 2)));
    }

    @Example
    void decimal_higherPrecisionZeroScale_isWidening() {
        // DECIMAL(5,0) → DECIMAL(18,0)
        assertTrue(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(5, 0), Types.DecimalType.of(18, 0)));
    }

    // =========================================================================
    // Narrowing pairs — must return false
    // =========================================================================

    @Example
    void longToInt_isNotWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.LongType.get(), Types.IntegerType.get()));
    }

    @Example
    void doubleToFloat_isNotWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DoubleType.get(), Types.FloatType.get()));
    }

    @Example
    void decimal_lowerPrecisionSameScale_isNotWidening() {
        // DECIMAL(12,2) → DECIMAL(10,2) — narrowing
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(12, 2), Types.DecimalType.of(10, 2)));
    }

    // =========================================================================
    // Scale change — must return false even if precision increases
    // =========================================================================

    @Example
    void decimal_differentScale_isNotWidening() {
        // DECIMAL(10,2) → DECIMAL(12,3) — scale changed, not allowed
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(10, 2), Types.DecimalType.of(12, 3)));
    }

    @Example
    void decimal_samePrecisionDifferentScale_isNotWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(10, 2), Types.DecimalType.of(10, 4)));
    }

    // =========================================================================
    // Identical types — must return false (no change)
    // =========================================================================

    @Example
    void intToInt_notWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.IntegerType.get(), Types.IntegerType.get()));
    }

    @Example
    void longToLong_notWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.LongType.get(), Types.LongType.get()));
    }

    @Example
    void floatToFloat_notWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.FloatType.get(), Types.FloatType.get()));
    }

    @Example
    void doubleToDouble_notWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DoubleType.get(), Types.DoubleType.get()));
    }

    @Example
    void decimal_samePrecisionSameScale_notWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(10, 2), Types.DecimalType.of(10, 2)));
    }

    // =========================================================================
    // Cross-family incompatible pairs — must return false
    // =========================================================================

    @Example
    void intToFloat_isNotWidening() {
        // Different families — not a defined widening
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.IntegerType.get(), Types.FloatType.get()));
    }

    @Example
    void intToDouble_isNotWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.IntegerType.get(), Types.DoubleType.get()));
    }

    @Example
    void stringToInt_isNotWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.StringType.get(), Types.IntegerType.get()));
    }

    @Example
    void intToString_isNotWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.IntegerType.get(), Types.StringType.get()));
    }

    @Example
    void longToDouble_isNotWidening() {
        // Even though LONG "fits" in DOUBLE at small values, this is not
        // defined as a safe widening in the design spec.
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.LongType.get(), Types.DoubleType.get()));
    }

    @Example
    void booleanToInt_isNotWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.BooleanType.get(), Types.IntegerType.get()));
    }

    @Example
    void decimalToLong_isNotWidening() {
        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(10, 0), Types.LongType.get()));
    }

    // =========================================================================
    // Null guard
    // =========================================================================

    @Example
    void nullFromType_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () ->
                TypeCompatibility.isTypeWidening(null, Types.LongType.get()));
    }

    @Example
    void nullToType_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () ->
                TypeCompatibility.isTypeWidening(Types.IntegerType.get(), null));
    }

    @Example
    void bothNull_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () ->
                TypeCompatibility.isTypeWidening(null, null));
    }

    // =========================================================================
    // Property: DECIMAL widening — higher precision, same scale → always true
    // =========================================================================

    /**
     * For any valid precision p in [1,37] and any target precision p' > p with
     * the same scale, the result must be {@code true}.
     */
    @Property(tries = 300)
    void property_decimalWideningHigherPrecision(
            @ForAll @IntRange(min = 1, max = 37) int fromPrecision,
            @ForAll @IntRange(min = 0, max = 9)  int scale) {

        // to-precision must be strictly greater and still valid (≤38)
        int toPrecision = fromPrecision + 1;
        if (toPrecision > 38) return; // skip edge

        // scale must not exceed precision
        if (scale > fromPrecision) return;

        assertTrue(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(fromPrecision, scale),
                Types.DecimalType.of(toPrecision,   scale)),
                "DECIMAL(" + fromPrecision + "," + scale + ") → DECIMAL("
                + toPrecision + "," + scale + ") should be widening");
    }

    /**
     * For any valid precision p, changing the scale (even slightly) is never
     * a widening.
     */
    @Property(tries = 200)
    void property_decimalScaleChange_neverWidening(
            @ForAll @IntRange(min = 2, max = 36) int precision,
            @ForAll @IntRange(min = 0, max = 8)  int fromScale) {

        int toScale = fromScale + 1;
        if (toScale > precision) return; // skip invalid Decimal

        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(precision, fromScale),
                Types.DecimalType.of(precision, toScale)),
                "Scale change from " + fromScale + " to " + toScale
                + " should not be widening");
    }

    /**
     * For any equal precision and scale, DECIMAL → DECIMAL with the same
     * dimensions is never a widening (no change).
     */
    @Property(tries = 200)
    void property_decimalSamePrecisionScale_notWidening(
            @ForAll @IntRange(min = 1, max = 38) int precision,
            @ForAll @IntRange(min = 0, max = 9)  int scale) {

        if (scale > precision) return;

        assertFalse(TypeCompatibility.isTypeWidening(
                Types.DecimalType.of(precision, scale),
                Types.DecimalType.of(precision, scale)),
                "DECIMAL(" + precision + "," + scale + ") → DECIMAL("
                + precision + "," + scale + ") should not be widening");
    }

    // =========================================================================
    // Property: known widening scalar pairs are always consistent
    // =========================================================================

    /**
     * Any of the three fixed scalar widening pairs must consistently return
     * {@code true} on repeated evaluation (idempotent).
     */
    @Property(tries = 100)
    void property_scalarWidenings_areIdempotent(
            @ForAll("scalarWideningPairs") Type[] pair) {

        // Must be true on every invocation regardless of iteration
        assertTrue(TypeCompatibility.isTypeWidening(pair[0], pair[1]),
                pair[0] + " → " + pair[1] + " should be widening");

        // Reverse must never be widening
        assertFalse(TypeCompatibility.isTypeWidening(pair[1], pair[0]),
                pair[1] + " → " + pair[0] + " (reverse) should not be widening");
    }

    @Provide
    Arbitrary<Type[]> scalarWideningPairs() {
        return Arbitraries.of(
                new Type[]{Types.IntegerType.get(), Types.LongType.get()},
                new Type[]{Types.FloatType.get(),   Types.DoubleType.get()}
        );
    }

    // =========================================================================
    // Property: every non-widening cross-family pair must return false
    // =========================================================================

    /**
     * String, Boolean, Date, Timestamp are never widenable to or from any
     * numeric type.
     */
    @Property(tries = 100)
    void property_nonNumericTypes_neverWidening(
            @ForAll("nonNumericTypes") Type nonNumeric,
            @ForAll("numericTypes")    Type numeric) {

        assertFalse(TypeCompatibility.isTypeWidening(nonNumeric, numeric),
                nonNumeric + " → " + numeric + " should not be widening");

        assertFalse(TypeCompatibility.isTypeWidening(numeric, nonNumeric),
                numeric + " → " + nonNumeric + " should not be widening");
    }

    @Provide
    Arbitrary<Type> nonNumericTypes() {
        return Arbitraries.of(
                Types.StringType.get(),
                Types.BooleanType.get(),
                Types.DateType.get(),
                Types.TimestampType.withZone(),
                Types.BinaryType.get()
        );
    }

    @Provide
    Arbitrary<Type> numericTypes() {
        return Arbitraries.of(
                Types.IntegerType.get(),
                Types.LongType.get(),
                Types.FloatType.get(),
                Types.DoubleType.get(),
                Types.DecimalType.of(10, 2)
        );
    }
}
