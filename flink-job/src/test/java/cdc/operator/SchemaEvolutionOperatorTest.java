package cdc.operator;

import cdc.model.ChangeEvent;
import cdc.model.DLQEnvelope;
import cdc.model.ErrorType;
import cdc.model.SourceInfo;
import cdc.schema.IcebergSchemaManager;
import cdc.schema.IncompatibleTypeChangeException;

import net.jqwik.api.*;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.StringLength;

import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based tests for {@link SchemaEvolutionOperator}.
 *
 * <p>Tags:
 * <ul>
 *   <li>{@code # Feature: cdc-pipeline, Property 6: Additive schema evolution propagates to Iceberg}</li>
 *   <li>{@code # Feature: cdc-pipeline, Property 7: Dropped column retained with NULL}</li>
 *   <li>{@code # Feature: cdc-pipeline, Property 8: Type widening updates Iceberg column type}</li>
 *   <li>{@code # Feature: cdc-pipeline, Property 9: Incompatible type change routes to DLQ}</li>
 * </ul>
 *
 * <p>All tests use an in-memory {@link StubSchemaManager} to avoid any dependency
 * on a live Iceberg REST catalog.
 *
 * <p>Requirements: 7.1, 7.2, 7.3, 7.4
 */
// Feature: cdc-pipeline, Properties 6-9: Schema evolution
class SchemaEvolutionOperatorTest {

    // =========================================================================
    // Stub schema manager — records calls in-memory; no catalog I/O
    // =========================================================================

    /**
     * In-memory test double for {@link IcebergSchemaManager}.
     * <p>
     * By default it records every {@link #applySchemaChange} call.
     * Configure {@link #throwOn} to inject a specific {@link IncompatibleTypeChangeException}.
     */
    static class StubSchemaManager extends IcebergSchemaManager {

        /** All (tableName, incomingFields) pairs passed to applySchemaChange. */
        final List<Map.Entry<String, Map<String, Type>>> calls = new ArrayList<>();

        /** If non-null, thrown on the next applySchemaChange invocation. */
        IncompatibleTypeChangeException throwOn = null;

        StubSchemaManager() {
            super("cdc");
        }

        @Override
        public void applySchemaChange(String tableName,
                                      Map<String, Type> incomingFields)
                throws IncompatibleTypeChangeException {
            calls.add(Map.entry(tableName, new HashMap<>(incomingFields)));
            if (throwOn != null) {
                IncompatibleTypeChangeException ex = throwOn;
                throwOn = null; // throw once
                throw ex;
            }
        }

        /** Returns the first recorded call's incomingFields, or empty map if none. */
        Map<String, Type> firstCallFields() {
            return calls.isEmpty() ? Collections.emptyMap() : calls.get(0).getValue();
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Builds a ChangeEvent whose after-image contains the given columns. */
    private static ChangeEvent eventWithAfter(Map<String, Object> after) {
        SourceInfo src = new SourceInfo(
                "2.5.0", "postgresql", "db", System.currentTimeMillis(),
                "sourcedb", "public", "orders", 100L, null);
        return new ChangeEvent(null, after, src, "c", src.ts_ms, null);
    }

    /** Builds a ChangeEvent whose before-image contains the given columns (DELETE). */
    private static ChangeEvent deleteEventWithBefore(Map<String, Object> before) {
        SourceInfo src = new SourceInfo(
                "2.5.0", "postgresql", "db", System.currentTimeMillis(),
                "sourcedb", "public", "orders", 200L, null);
        return new ChangeEvent(before, null, src, "d", src.ts_ms, null);
    }

    /** Opens the operator with a pre-injected stub manager; avoids live catalog. */
    private static SchemaEvolutionOperator openWithStub(StubSchemaManager stub)
            throws Exception {
        SchemaEvolutionOperator op =
                new SchemaEvolutionOperator("http://localhost:8181", "s3://warehouse/", "cdc");
        op.setSchemaManager(stub);
        op.open(new Configuration());
        return op;
    }

    // =========================================================================
    // Property 6: Additive schema evolution propagates to Iceberg
    // =========================================================================

    /**
     * For any new column (name + type), the operator must call
     * {@link IcebergSchemaManager#applySchemaChange} with a field map that
     * contains that column, and the event must be passed through unchanged.
     *
     * <p>Tag: {@code # Feature: cdc-pipeline, Property 6: Additive schema evolution propagates to Iceberg}
     */
    @Property(tries = 200)
    // Feature: cdc-pipeline, Property 6: Additive schema evolution propagates to Iceberg
    void p6_newColumn_propagatesToIceberg(
            @ForAll @AlphaChars @StringLength(min = 1, max = 20) String columnName,
            @ForAll("icebergTypes") Type columnType) throws Exception {

        // Skip column names that clash with reserved CDC metadata columns.
        if (columnName.startsWith("_cdc")) return;

        StubSchemaManager stub = new StubSchemaManager();
        SchemaEvolutionOperator op = openWithStub(stub);

        Map<String, Object> after = new HashMap<>();
        after.put("id", 42);
        after.put(columnName, sampleValueForType(columnType));

        ChangeEvent event  = eventWithAfter(after);
        ChangeEvent result = op.map(event);

        // Event must be forwarded (not null).
        assertNotNull(result, "Event should be forwarded when schema evolution succeeds");
        assertSame(event, result, "Operator should return the same event instance");

        // SchemaManager must have been called with the new column.
        assertFalse(stub.calls.isEmpty(), "applySchemaChange should have been called");
        assertTrue(stub.firstCallFields().containsKey(columnName),
                "Schema call should include new column '" + columnName + "'");

        // No DLQ envelope on success.
        assertNull(op.takePendingDlqEnvelope(), "No DLQ envelope expected on success");
    }

    // =========================================================================
    // Property 7: Dropped column retained with NULL in Iceberg
    // =========================================================================

    /**
     * When a column is absent from the event's row image (simulating a DROP),
     * the operator must still call applySchemaChange for the columns that ARE
     * present (it does not need to see the missing column), and the event must
     * flow through.  The retention-with-NULL guarantee lives in Iceberg itself;
     * the operator's responsibility is simply not to remove the column.
     *
     * <p>This property verifies that the operator does NOT add a null/removal
     * directive for the dropped column — i.e. it only submits columns actually
     * present in the row image.
     *
     * <p>Tag: {@code # Feature: cdc-pipeline, Property 7: Dropped column retained with NULL}
     */
    @Property(tries = 200)
    // Feature: cdc-pipeline, Property 7: Dropped column retained with NULL
    void p7_droppedColumn_notIncludedInSchemaUpdate(
            @ForAll @AlphaChars @StringLength(min = 1, max = 15) String droppedColumn,
            @ForAll @AlphaChars @StringLength(min = 1, max = 15) String retainedColumn)
            throws Exception {

        if (droppedColumn.equals(retainedColumn)) return; // skip degenerate case

        StubSchemaManager stub = new StubSchemaManager();
        SchemaEvolutionOperator op = openWithStub(stub);

        // After-image does NOT contain droppedColumn — it was dropped from the source.
        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        after.put(retainedColumn, "value");
        // droppedColumn intentionally absent

        ChangeEvent result = op.map(eventWithAfter(after));

        assertNotNull(result, "Event should flow through");
        assertFalse(stub.calls.isEmpty());

        Map<String, Type> submitted = stub.firstCallFields();
        // The dropped column must NOT appear in the schema update call.
        assertFalse(submitted.containsKey(droppedColumn),
                "Dropped column '" + droppedColumn
                + "' must not appear in schema update call");

        // The retained column must be submitted.
        assertTrue(submitted.containsKey(retainedColumn),
                "Retained column '" + retainedColumn
                + "' must appear in schema update call");

        assertNull(op.takePendingDlqEnvelope());
    }

    // =========================================================================
    // Property 8: Type widening updates Iceberg column type
    // =========================================================================

    /**
     * For any valid widening pair, the operator must call applySchemaChange with
     * the new wider type, the event must pass through, and no DLQ envelope
     * must be generated.
     *
     * <p>Tag: {@code # Feature: cdc-pipeline, Property 8: Type widening updates Iceberg column type}
     */
    @Property(tries = 100)
    // Feature: cdc-pipeline, Property 8: Type widening updates Iceberg column type
    void p8_typeWidening_propagatesAndNoDataLoss(
            @ForAll("wideningPairs") Type[] pair) throws Exception {

        // pair[0] = from-type (represented by a Java value), pair[1] = to-type
        StubSchemaManager stub = new StubSchemaManager();
        SchemaEvolutionOperator op = openWithStub(stub);

        // The after-image carries a value that maps to the WIDER (to) type.
        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        after.put("amount", sampleValueForType(pair[1]));

        ChangeEvent result = op.map(eventWithAfter(after));

        // Event must be forwarded.
        assertNotNull(result, "Event with widened type should be forwarded");

        // SchemaManager must have seen the wider type for the column.
        assertFalse(stub.calls.isEmpty());
        Type submittedType = stub.firstCallFields().get("amount");
        assertNotNull(submittedType, "Column 'amount' should appear in schema update");
        assertEquals(pair[1], submittedType,
                "Schema update should use the wider type " + pair[1]);

        // No DLQ envelope.
        assertNull(op.takePendingDlqEnvelope(), "No DLQ envelope expected for valid widening");
    }

    // =========================================================================
    // Property 9: Incompatible type change routes to DLQ
    // =========================================================================

    /**
     * When {@link IcebergSchemaManager#applySchemaChange} throws
     * {@link IncompatibleTypeChangeException}, the operator must:
     * <ol>
     *   <li>Return {@code null} (event not forwarded downstream)</li>
     *   <li>Populate a {@link DLQEnvelope} with {@code error_type = TYPE_MISMATCH}</li>
     *   <li>Set all mandatory DLQ fields to non-null/non-empty values</li>
     * </ol>
     *
     * <p>Tag: {@code # Feature: cdc-pipeline, Property 9: Incompatible type change routes to DLQ}
     */
    @Property(tries = 200)
    // Feature: cdc-pipeline, Property 9: Incompatible type change routes to DLQ
    void p9_incompatibleTypeChange_routesToDlq(
            @ForAll @AlphaChars @StringLength(min = 1, max = 20) String columnName)
            throws Exception {

        if (columnName.startsWith("_cdc")) return;

        StubSchemaManager stub = new StubSchemaManager();
        // Inject the exception that will be thrown on the next applySchemaChange call.
        stub.throwOn = new IncompatibleTypeChangeException(
                columnName,
                Types.LongType.get(),   // current type
                Types.StringType.get()  // incoming incompatible type
        );

        SchemaEvolutionOperator op = openWithStub(stub);

        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        after.put(columnName, "incompatible_value");

        ChangeEvent result = op.map(eventWithAfter(after));

        // Event must NOT be forwarded.
        assertNull(result, "Event with incompatible type change must not be forwarded");

        // A DLQ envelope must be available.
        DLQEnvelope envelope = op.takePendingDlqEnvelope();
        assertNotNull(envelope, "DLQ envelope must be populated for TYPE_MISMATCH");

        // Error type must be TYPE_MISMATCH.
        assertEquals(ErrorType.TYPE_MISMATCH, envelope.error_type,
                "DLQ error_type must be TYPE_MISMATCH");

        // All mandatory DLQ fields must be non-null / non-empty.
        assertNotNull(envelope.original_topic,     "original_topic must not be null");
        assertFalse(envelope.original_topic.isEmpty(), "original_topic must not be empty");
        assertNotNull(envelope.error_message,      "error_message must not be null");
        assertFalse(envelope.error_message.isEmpty(), "error_message must not be empty");
        assertTrue(envelope.error_timestamp_utc > 0, "error_timestamp_utc must be positive");
        assertNotNull(envelope.raw_payload,        "raw_payload must not be null");
        assertTrue(envelope.raw_payload.length > 0, "raw_payload must not be empty");

        // A second takePendingDlqEnvelope must return null (cleared after take).
        assertNull(op.takePendingDlqEnvelope(), "pendingDlqEnvelope must be cleared after take");
    }

    // =========================================================================
    // Additional corner-case examples
    // =========================================================================

    /**
     * DELETE event (null after-image, non-null before-image): operator must
     * derive column types from the before-image and not throw.
     */
    @Example
    void deleteEvent_beforeImage_usedForSchemaCheck() throws Exception {
        StubSchemaManager stub = new StubSchemaManager();
        SchemaEvolutionOperator op = openWithStub(stub);

        Map<String, Object> before = Map.of("id", 99, "status", "active");
        ChangeEvent result = op.map(deleteEventWithBefore(before));

        assertNotNull(result, "DELETE event should be forwarded");
        assertFalse(stub.calls.isEmpty(), "applySchemaChange should be called for DELETE");
    }

    /**
     * Event with both before and after null (highly degenerate): operator
     * must not throw and must pass the event through.
     */
    @Example
    void eventWithNoImage_passedThrough() throws Exception {
        StubSchemaManager stub = new StubSchemaManager();
        SchemaEvolutionOperator op = openWithStub(stub);

        SourceInfo src = new SourceInfo("2.5.0", "postgresql", "db",
                System.currentTimeMillis(), "sourcedb", "public", "orders", 1L, null);
        ChangeEvent event = new ChangeEvent(null, null, src, "t", src.ts_ms, null);

        ChangeEvent result = op.map(event);

        assertNotNull(result, "Degenerate event with no image should be passed through");
    }

    /**
     * An event with null source metadata should not throw and should pass through.
     */
    @Example
    void eventWithNullSource_passedThrough() throws Exception {
        StubSchemaManager stub = new StubSchemaManager();
        SchemaEvolutionOperator op = openWithStub(stub);

        ChangeEvent event = new ChangeEvent(null, Map.of("id", 1), null, "c", null, null);
        ChangeEvent result = op.map(event);

        assertNotNull(result, "Event with null source should be passed through");
        assertTrue(stub.calls.isEmpty(),
                "No schema call should be made when source is null");
    }

    /**
     * pendingDlqEnvelope is null by default (no error before first call).
     */
    @Example
    void noPendingEnvelope_beforeFirstCall() throws Exception {
        StubSchemaManager stub = new StubSchemaManager();
        SchemaEvolutionOperator op = openWithStub(stub);
        assertNull(op.takePendingDlqEnvelope());
    }

    /**
     * Successive successful calls must not accumulate envelopes.
     */
    @Example
    void successiveCalls_noEnvelopeAccumulation() throws Exception {
        StubSchemaManager stub = new StubSchemaManager();
        SchemaEvolutionOperator op = openWithStub(stub);

        ChangeEvent e1 = eventWithAfter(Map.of("id", 1, "name", "alice"));
        ChangeEvent e2 = eventWithAfter(Map.of("id", 2, "name", "bob"));

        op.map(e1);
        assertNull(op.takePendingDlqEnvelope());
        op.map(e2);
        assertNull(op.takePendingDlqEnvelope());
    }

    // =========================================================================
    // Generators
    // =========================================================================

    /** Supplies a representative set of Iceberg primitive types. */
    @Provide
    Arbitrary<Type> icebergTypes() {
        return Arbitraries.of(
                Types.IntegerType.get(),
                Types.LongType.get(),
                Types.FloatType.get(),
                Types.DoubleType.get(),
                Types.BooleanType.get(),
                Types.StringType.get(),
                Types.DecimalType.of(10, 2),
                Types.DecimalType.of(18, 4)
        );
    }

    /** Supplies the three valid widening pairs as [fromType, toType] arrays. */
    @Provide
    Arbitrary<Type[]> wideningPairs() {
        return Arbitraries.of(
                new Type[]{Types.IntegerType.get(), Types.LongType.get()},
                new Type[]{Types.FloatType.get(),   Types.DoubleType.get()},
                new Type[]{Types.DecimalType.of(10, 2), Types.DecimalType.of(15, 2)}
        );
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Returns a canonical non-null Java value that maps to the given Iceberg type
     * via {@link SchemaEvolutionOperator#javaToIcebergType}.
     */
    private static Object sampleValueForType(Type type) {
        Type.TypeID id = type.typeId();
        if (id == Type.TypeID.INTEGER) return 1;
        if (id == Type.TypeID.LONG)    return 1L;
        if (id == Type.TypeID.FLOAT)   return 1.0f;
        if (id == Type.TypeID.DOUBLE)  return 1.0d;
        if (id == Type.TypeID.BOOLEAN) return true;
        if (id == Type.TypeID.DECIMAL) return new java.math.BigDecimal("123.45");
        if (id == Type.TypeID.BINARY)  return new byte[]{0x01};
        return "sample";
    }
}
