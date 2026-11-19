package cdc;

import cdc.model.ChangeEvent;
import cdc.model.DLQEnvelope;
import cdc.model.ErrorType;
import cdc.model.PrimaryKey;
import cdc.model.SourceInfo;
import cdc.operator.ChangeEventRouter;
import cdc.operator.DeduplicationOperator;
import cdc.operator.DLQRouter;
import cdc.operator.SchemaEvolutionOperator;
import cdc.schema.IcebergSchemaManager;
import cdc.schema.IncompatibleTypeChangeException;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Positive;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based tests for the CDC pipeline operator graph.
 *
 * <p>Tag: {@code # Feature: cdc-pipeline, Property 3: Upsert/delete routing}
 *
 * <p>Requirements: 5.1, 5.2
 */
// Feature: cdc-pipeline, Property 3: Upsert/delete routing
class CdcPipelineJobTest {

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Unwraps a side-output queue of StreamRecords into a plain List of values. */
    private static <T> List<T> sideOutput(
            OneInputStreamOperatorTestHarness<?, ?> harness,
            OutputTag<T> tag) {
        ConcurrentLinkedQueue<StreamRecord<T>> queue = harness.getSideOutput(tag);
        if (queue == null) return Collections.emptyList();
        return queue.stream().map(StreamRecord::getValue).collect(Collectors.toList());
    }

    /** Stub: schema always accepted; no catalog I/O. */
    static class NoOpSchemaManager extends IcebergSchemaManager {
        NoOpSchemaManager() { super("cdc"); }
        @Override
        public void applySchemaChange(String t,
                Map<String, org.apache.iceberg.types.Type> f) { /* no-op */ }
    }

    /** Stub: always throws TYPE_MISMATCH. */
    static class TypeMismatchSchemaManager extends IcebergSchemaManager {
        final String column;
        TypeMismatchSchemaManager(String column) { super("cdc"); this.column = column; }
        @Override
        public void applySchemaChange(String t,
                Map<String, org.apache.iceberg.types.Type> f)
                throws IncompatibleTypeChangeException {
            throw new IncompatibleTypeChangeException(column,
                    org.apache.iceberg.types.Types.LongType.get(),
                    org.apache.iceberg.types.Types.StringType.get());
        }
    }

    private static ChangeEvent makeEvent(int id, String op, String table) {
        SourceInfo src = new SourceInfo("2.5.0", "postgresql", "db",
                System.currentTimeMillis(), "sourcedb", "public", table,
                (long) id * 100, null);
        Map<String, Object> after  = (!"d".equals(op)) ? new HashMap<>() : null;
        Map<String, Object> before = new HashMap<>();
        before.put("id", id);
        if (after != null) { after.put("id", id); after.put("status", "active"); }
        return new ChangeEvent(before, after, src, op, src.ts_ms, null);
    }

    private static SchemaEvolutionOperator noOpSchemaOp() {
        SchemaEvolutionOperator op = new SchemaEvolutionOperator(
                "http://localhost:8181", "s3://warehouse/", "cdc");
        op.setSchemaManager(new NoOpSchemaManager());
        return op;
    }

    private static SchemaEvolutionOperator typeMismatchSchemaOp(String col) {
        SchemaEvolutionOperator op = new SchemaEvolutionOperator(
                "http://localhost:8181", "s3://warehouse/", "cdc");
        op.setSchemaManager(new TypeMismatchSchemaManager(col));
        return op;
    }

    private KeyedOneInputStreamOperatorTestHarness<PrimaryKey, ChangeEvent, ChangeEvent>
    dedupHarness() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<PrimaryKey, ChangeEvent, ChangeEvent> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new DeduplicationOperator(60_000L)),
                        e -> e.primaryKey("id"),
                        Types.POJO(PrimaryKey.class));
        h.open();
        return h;
    }

    private OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent>
    schemaHarness(SchemaEvolutionOperator op) throws Exception {
        // Wrap the SchemaEvolutionOperator in a FlatMapFunction to handle nulls
        org.apache.flink.api.common.functions.RichFlatMapFunction<ChangeEvent, ChangeEvent> wrapper =
                new org.apache.flink.api.common.functions.RichFlatMapFunction<ChangeEvent, ChangeEvent>() {
                    @Override
                    public void open(org.apache.flink.configuration.Configuration conf) throws Exception {
                        op.open(conf);
                    }
                    @Override
                    public void flatMap(ChangeEvent event, org.apache.flink.util.Collector<ChangeEvent> out) throws Exception {
                        ChangeEvent result = op.map(event);
                        if (result != null) {
                            out.collect(result);
                        }
                    }
                };
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> h =
                new OneInputStreamOperatorTestHarness<>(
                        new org.apache.flink.streaming.api.operators.StreamFlatMap<>(wrapper));
        h.open();
        return h;
    }

    private OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent>
    dlqRouterHarness(DLQRouter router) throws Exception {
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> h =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<ChangeEvent, ChangeEvent>(router));
        h.open();
        return h;
    }

    private OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent>
    routerHarness() throws Exception {
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> h =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<ChangeEvent, ChangeEvent>(new ChangeEventRouter()));
        h.open();
        return h;
    }

    // =========================================================================
    // Property 3a: INSERT/UPDATE — after-image intact through full sub-pipeline
    // =========================================================================

    @Property(tries = 300)
    // Feature: cdc-pipeline, Property 3: Upsert/delete routing
    void p3a_insertUpdateEvent_afterImagePreserved(
            @ForAll @IntRange(min = 1, max = 10_000) int id,
            @ForAll("upsertOps") String op) throws Exception {

        ChangeEvent ev = makeEvent(id, op, "orders");

        KeyedOneInputStreamOperatorTestHarness<PrimaryKey, ChangeEvent, ChangeEvent> dedup =
                dedupHarness();
        try {
            dedup.processElement(ev, 0L);
            List<ChangeEvent> dedupOut = dedup.extractOutputValues();
            assertFalse(dedupOut.isEmpty(), "Dedup must forward " + op);
            ChangeEvent afterDedup = dedupOut.get(0);

            OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> schema =
                    schemaHarness(noOpSchemaOp());
            try {
                schema.processElement(afterDedup, 0L);
                List<ChangeEvent> schemaOut = schema.extractOutputValues();
                // SchemaEvolutionOperator is a map — null return is not emitted
                assertFalse(schemaOut.isEmpty(),
                        "SchemaEvolution must forward " + op + " event");
                ChangeEvent afterSchema = schemaOut.get(0);

                DLQRouter router = new DLQRouter();
                OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> dlq =
                        dlqRouterHarness(router);
                try {
                    dlq.processElement(afterSchema, 0L);
                    List<ChangeEvent> routerOut = dlq.extractOutputValues();
                    assertEquals(1, routerOut.size(), op + " must reach sink stream");
                    ChangeEvent sinkEvent = routerOut.get(0);
                    assertEquals(op, sinkEvent.op);
                    assertNotNull(sinkEvent.after, "after-image must be non-null for " + op);
                    assertEquals(id, sinkEvent.after.get("id"));
                    List<DLQEnvelope> dlqEnv = sideOutput(dlq, DLQRouter.DLQ_OUTPUT_TAG);
                    assertTrue(dlqEnv.isEmpty(), "No DLQ envelope on success");
                } finally { dlq.close(); }
            } finally { schema.close(); }
        } finally { dedup.close(); }
    }

    // =========================================================================
    // Property 3b: DELETE — null after-image in output
    // =========================================================================

    @Property(tries = 200)
    // Feature: cdc-pipeline, Property 3: Upsert/delete routing
    void p3b_deleteEvent_nullAfterImage(
            @ForAll @IntRange(min = 1, max = 10_000) int id) throws Exception {

        ChangeEvent ev = makeEvent(id, "d", "orders");

        KeyedOneInputStreamOperatorTestHarness<PrimaryKey, ChangeEvent, ChangeEvent> dedup =
                dedupHarness();
        try {
            dedup.processElement(ev, 0L);
            List<ChangeEvent> dedupOut = dedup.extractOutputValues();
            assertFalse(dedupOut.isEmpty(), "Dedup must forward DELETE");

            OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> schema =
                    schemaHarness(noOpSchemaOp());
            try {
                schema.processElement(dedupOut.get(0), 0L);
                List<ChangeEvent> schemaOut = schema.extractOutputValues();
                assertFalse(schemaOut.isEmpty(), "Schema must forward DELETE");

                DLQRouter router = new DLQRouter();
                OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> dlq =
                        dlqRouterHarness(router);
                try {
                    dlq.processElement(schemaOut.get(0), 0L);
                    List<ChangeEvent> routerOut = dlq.extractOutputValues();
                    assertEquals(1, routerOut.size(), "DELETE must reach sink stream");
                    assertEquals("d", routerOut.get(0).op);
                    assertNull(routerOut.get(0).after, "after-image must be null for DELETE");
                    assertNotNull(routerOut.get(0).before);
                } finally { dlq.close(); }
            } finally { schema.close(); }
        } finally { dedup.close(); }
    }

    // =========================================================================
    // Property 3c: snapshot (r) passes through dedup
    // =========================================================================

    @Property(tries = 100)
    void p3c_snapshotEvent_passesThrough(
            @ForAll @IntRange(min = 1, max = 10_000) int id) throws Exception {

        ChangeEvent ev = makeEvent(id, "r", "orders");
        KeyedOneInputStreamOperatorTestHarness<PrimaryKey, ChangeEvent, ChangeEvent> dedup =
                dedupHarness();
        try {
            dedup.processElement(ev, 0L);
            List<ChangeEvent> out = dedup.extractOutputValues();
            assertFalse(out.isEmpty(), "Snapshot event must pass dedup");
            assertEquals("r", out.get(0).op);
        } finally { dedup.close(); }
    }

    // =========================================================================
    // Property: ChangeEventRouter routes to correct per-table side output
    // =========================================================================

    @Property(tries = 200)
    void router_routesToCorrectTable(
            @ForAll("tableNames") String table,
            @ForAll("allOps")     String op,
            @ForAll @IntRange(min = 1, max = 1000) int id) throws Exception {

        ChangeEvent ev = makeEvent(id, op, table);
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                routerHarness();
        try {
            harness.processElement(ev, 0L);

            assertTrue(harness.extractOutputValues().isEmpty(),
                    "Known table must not reach main output");

            OutputTag<ChangeEvent> expectedTag = ChangeEventRouter.tagForTable(table);
            assertNotNull(expectedTag);
            List<ChangeEvent> sideOut = sideOutput(harness, expectedTag);
            assertEquals(1, sideOut.size());
            assertEquals(table, sideOut.get(0).source.table);

            for (String other : List.of("orders", "customers", "products")) {
                if (!other.equals(table)) {
                    List<ChangeEvent> otherOut =
                            sideOutput(harness, ChangeEventRouter.tagForTable(other));
                    assertTrue(otherOut.isEmpty(),
                            "Event must not appear in '" + other + "' stream");
                }
            }
        } finally { harness.close(); }
    }

    // =========================================================================
    // Example: unknown table reaches main output
    // =========================================================================

    @Example
    void router_unknownTable_reachesMainOutput() throws Exception {
        ChangeEvent ev = makeEvent(1, "c", "unknown_table");
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                routerHarness();
        try {
            harness.processElement(ev, 0L);
            assertEquals(1, harness.extractOutputValues().size(),
                    "Unknown table event must reach main output");
        } finally { harness.close(); }
    }

    // =========================================================================
    // Property: TYPE_MISMATCH removes event from main stream
    // =========================================================================

    @Property(tries = 100)
    void typeMismatch_eventRemovedFromMainStream(
            @ForAll @IntRange(min = 1, max = 1000) int id,
            @ForAll("upsertOps") String op) throws Exception {

        ChangeEvent ev = makeEvent(id, op, "orders");
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> schema =
                schemaHarness(typeMismatchSchemaOp("amount"));
        try {
            schema.processElement(ev, 0L);
            // StreamMap does not forward null returns
            List<ChangeEvent> out = schema.extractOutputValues();
            assertTrue(out.isEmpty(),
                    "TYPE_MISMATCH event must not pass schema evolution");
        } finally { schema.close(); }
    }

    // =========================================================================
    // Property: multiple PKs independently deduplicated
    // =========================================================================

    @Property(tries = 100)
    void multipleKeys_independentlyDeduplicated(
            @ForAll @IntRange(min = 2, max = 20) int numKeys) throws Exception {

        List<ChangeEvent> events = new ArrayList<>();
        for (int i = 1; i <= numKeys; i++) events.add(makeEvent(i, "c", "orders"));

        KeyedOneInputStreamOperatorTestHarness<PrimaryKey, ChangeEvent, ChangeEvent> dedup =
                dedupHarness();
        try {
            long t = 0;
            for (ChangeEvent e : events) dedup.processElement(e, t++);
            List<ChangeEvent> out = dedup.extractOutputValues();
            assertEquals(numKeys, out.size(), "Each PK must produce exactly one event");
            Set<Object> seenPks = new HashSet<>();
            for (ChangeEvent e : out) assertTrue(seenPks.add(e.after.get("id")));
        } finally { dedup.close(); }
    }

    // =========================================================================
    // Property: DLQEnvelopeSerializer produces valid JSON
    // =========================================================================

    @Property(tries = 100)
    void dlqSerializer_producesNonEmptyBytes(
            @ForAll("errorTypes") ErrorType errorType) throws Exception {

        cdc.sink.DLQEnvelopeSerializer ser = new cdc.sink.DLQEnvelopeSerializer();
        ser.open(null);
        DLQEnvelope env = new DLQEnvelope("db.public.orders", 0, 42L,
                errorType, "test error", System.currentTimeMillis(),
                "payload".getBytes());
        byte[] bytes = ser.serialize(env);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
        String json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(json.contains(errorType.name()),
                "JSON must contain error_type: " + errorType.name());
    }

    // =========================================================================
    // Example: CdcPipelineJob constants are self-consistent
    // =========================================================================

    @Example
    void jobConstants_areValid() {
        assertTrue(CdcPipelineJob.PARALLELISM > 0);
        assertTrue(CdcPipelineJob.CHECKPOINT_INTERVAL > 0);
        assertTrue(CdcPipelineJob.CHECKPOINT_TIMEOUT > CdcPipelineJob.CHECKPOINT_INTERVAL / 2);
        assertEquals(1, CdcPipelineJob.MAX_CONCURRENT_CPS);
        assertTrue(CdcPipelineJob.TOLERATED_CP_FAILURES >= 0);
        assertEquals(3, CdcPipelineJob.CDC_TOPICS.size());
    }

    // =========================================================================
    // Generators
    // =========================================================================

    @Provide Arbitrary<String> upsertOps()  { return Arbitraries.of("c", "u"); }
    @Provide Arbitrary<String> allOps()     { return Arbitraries.of("c", "u", "d", "r"); }
    @Provide Arbitrary<String> tableNames() { return Arbitraries.of("orders", "customers", "products"); }
    @Provide Arbitrary<ErrorType> errorTypes() { return Arbitraries.of(ErrorType.values()); }
}
