package cdc.operator;

import cdc.model.ChangeEvent;
import cdc.model.DLQEnvelope;
import cdc.model.ErrorType;
import cdc.model.SourceInfo;

import net.jqwik.api.*;
import net.jqwik.api.constraints.Positive;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based tests for {@link DLQRouter}.
 *
 * <p>Tag: {@code # Feature: cdc-pipeline, Property 10: DLQ metadata completeness}
 *
 * <p>Requirements: 8.1, 8.2
 */
// Feature: cdc-pipeline, Property 10: DLQ metadata completeness
class DLQRouterTest {

    // =========================================================================
    // Harness + side-output helpers
    // =========================================================================

    private OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> buildHarness(
            DLQRouter router) throws Exception {
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<ChangeEvent, ChangeEvent>(router));
        harness.open();
        return harness;
    }

    /** Unwraps a side-output queue into a plain List. */
    private static <T> List<T> sideOutput(
            OneInputStreamOperatorTestHarness<?, ?> harness,
            OutputTag<T> tag) {
        ConcurrentLinkedQueue<StreamRecord<T>> queue = harness.getSideOutput(tag);
        if (queue == null) return Collections.emptyList();
        return queue.stream().map(StreamRecord::getValue).collect(Collectors.toList());
    }

    // =========================================================================
    // Event builders
    // =========================================================================

    private static ChangeEvent event(String table, String op) {
        SourceInfo src = new SourceInfo("2.5.0", "postgresql", "db",
                System.currentTimeMillis(), "sourcedb", "public", table, 100L, null);
        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        return new ChangeEvent(null, after, src, op, src.ts_ms, null);
    }

    private static ChangeEvent deleteEvent(String table) {
        SourceInfo src = new SourceInfo("2.5.0", "postgresql", "db",
                System.currentTimeMillis(), "sourcedb", "public", table, 200L, null);
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        return new ChangeEvent(before, null, src, "d", src.ts_ms, null);
    }

    // =========================================================================
    // Property 10: DLQ messages contain complete metadata
    // =========================================================================

    @Property(tries = 200)
    // Feature: cdc-pipeline, Property 10: DLQ metadata completeness
    void p10_allDlqFieldsPopulated(
            @ForAll("errorTypes") ErrorType errorType,
            @ForAll("tableNames") String    tableName,
            @ForAll("opCodes")    String    op,
            @ForAll @Positive     int       partition,
            @ForAll @Positive     long      offset) throws Exception {

        DLQRouter router = new DLQRouter();
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                buildHarness(router);
        try {
            ChangeEvent ev = event(tableName, op);
            router.pendingError = new DLQRouter.ErrorContext(
                    errorType, "Test error: " + errorType, partition, offset);
            harness.processElement(ev, 0L);

            // Main output must be empty.
            assertTrue(harness.extractOutputValues().isEmpty(),
                    "Error event must NOT reach main output");

            List<DLQEnvelope> dlqOut = sideOutput(harness, DLQRouter.DLQ_OUTPUT_TAG);
            assertEquals(1, dlqOut.size(), "Exactly one DLQ envelope expected");

            DLQEnvelope env = dlqOut.get(0);
            assertNotNull(env.original_topic);
            assertFalse(env.original_topic.isEmpty(),  "original_topic must not be empty");
            assertNotNull(env.error_type);
            assertEquals(errorType, env.error_type,    "error_type must match");
            assertNotNull(env.error_message);
            assertFalse(env.error_message.isEmpty(),   "error_message must not be empty");
            assertTrue(env.error_timestamp_utc > 0,    "error_timestamp_utc must be positive");
            assertNotNull(env.raw_payload);
            assertTrue(env.raw_payload.length > 0,     "raw_payload must not be empty");
        } finally {
            harness.close();
        }
    }

    // =========================================================================
    // Property: success path
    // =========================================================================

    @Property(tries = 200)
    void successPath_eventReachesMainOutput(
            @ForAll("tableNames") String tableName,
            @ForAll("opCodes")    String op) throws Exception {

        DLQRouter router = new DLQRouter();
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                buildHarness(router);
        try {
            ChangeEvent ev = event(tableName, op);
            harness.processElement(ev, 0L);

            List<ChangeEvent> mainOut = harness.extractOutputValues();
            assertEquals(1, mainOut.size(), "Event must reach main output on success");
            assertEquals(ev.toString(), mainOut.get(0).toString());

            List<DLQEnvelope> dlqOut = sideOutput(harness, DLQRouter.DLQ_OUTPUT_TAG);
            assertTrue(dlqOut.isEmpty(), "No DLQ envelope on success");
        } finally {
            harness.close();
        }
    }

    // =========================================================================
    // Property: correct topic derivation
    // =========================================================================

    @Property(tries = 100)
    void topicDerivedFromSource(
            @ForAll("tableNames") String tableName) throws Exception {

        DLQRouter router = new DLQRouter();
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                buildHarness(router);
        try {
            ChangeEvent ev = event(tableName, "c");
            router.pendingError = new DLQRouter.ErrorContext(
                    ErrorType.PROCESSING_ERROR, "err", 0, 0L);
            harness.processElement(ev, 0L);

            List<DLQEnvelope> dlqOut = sideOutput(harness, DLQRouter.DLQ_OUTPUT_TAG);
            assertEquals(1, dlqOut.size());
            String expectedTopic = ev.source.name + "." + ev.source.schema + "." + ev.source.table;
            assertEquals(expectedTopic, dlqOut.get(0).original_topic);
        } finally {
            harness.close();
        }
    }

    // =========================================================================
    // Property: pendingError cleared after routing
    // =========================================================================

    @Property(tries = 100)
    void pendingErrorClearedAfterRoute(
            @ForAll("errorTypes") ErrorType errorType) throws Exception {

        DLQRouter router = new DLQRouter();
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                buildHarness(router);
        try {
            ChangeEvent first = event("orders", "c");
            router.pendingError = new DLQRouter.ErrorContext(errorType, "error", 0, 0L);
            harness.processElement(first, 0L);

            ChangeEvent second = event("orders", "u");
            harness.processElement(second, 1L);

            List<ChangeEvent> mainOut = harness.extractOutputValues();
            assertEquals(1, mainOut.size(), "Only the second event reaches main output");
            assertEquals(second.toString(), mainOut.get(0).toString());

            List<DLQEnvelope> dlqOut = sideOutput(harness, DLQRouter.DLQ_OUTPUT_TAG);
            assertEquals(1, dlqOut.size(), "Only the first event in DLQ");
            assertNull(router.pendingError, "pendingError must be null after routing");
        } finally {
            harness.close();
        }
    }

    // =========================================================================
    // Property: all four ErrorType values produce valid envelopes
    // =========================================================================

    @Property(tries = 100)
    void allErrorTypes_produceValidEnvelopes(
            @ForAll("errorTypes") ErrorType errorType) throws Exception {

        DLQRouter router = new DLQRouter();
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                buildHarness(router);
        try {
            ChangeEvent ev = event("orders", "c");
            router.pendingError = new DLQRouter.ErrorContext(
                    errorType, errorType.name() + " description", 2, 9999L);
            harness.processElement(ev, 0L);

            List<DLQEnvelope> dlqOut = sideOutput(harness, DLQRouter.DLQ_OUTPUT_TAG);
            assertEquals(1, dlqOut.size());
            assertEquals(errorType, dlqOut.get(0).error_type);
            assertEquals(2,     dlqOut.get(0).original_partition);
            assertEquals(9999L, dlqOut.get(0).original_offset);
        } finally {
            harness.close();
        }
    }

    // =========================================================================
    // Property: DELETE events produce complete envelopes
    // =========================================================================

    @Property(tries = 100)
    void deleteEvents_completeEnvelope(
            @ForAll("tableNames") String tableName) throws Exception {

        DLQRouter router = new DLQRouter();
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                buildHarness(router);
        try {
            ChangeEvent ev = deleteEvent(tableName);
            router.pendingError = new DLQRouter.ErrorContext(
                    ErrorType.TYPE_MISMATCH, "delete routing error", 0, 0L);
            harness.processElement(ev, 0L);

            List<DLQEnvelope> dlqOut = sideOutput(harness, DLQRouter.DLQ_OUTPUT_TAG);
            assertFalse(dlqOut.isEmpty(), "DELETE event must produce DLQ envelope");
            assertNotNull(dlqOut.get(0).raw_payload);
            assertTrue(dlqOut.get(0).raw_payload.length > 0);
        } finally {
            harness.close();
        }
    }

    // =========================================================================
    // @Example corner cases
    // =========================================================================

    @Example
    void nullSourceEvent_doesNotThrow() throws Exception {
        DLQRouter router = new DLQRouter();
        OneInputStreamOperatorTestHarness<ChangeEvent, ChangeEvent> harness =
                buildHarness(router);
        try {
            ChangeEvent ev = new ChangeEvent(null, null, null, "c", null, null);
            router.pendingError = new DLQRouter.ErrorContext(
                    ErrorType.PROCESSING_ERROR, "null source", 0, 0L);
            harness.processElement(ev, 0L);
            List<DLQEnvelope> dlqOut = sideOutput(harness, DLQRouter.DLQ_OUTPUT_TAG);
            assertEquals(1, dlqOut.size());
            assertEquals("unknown", dlqOut.get(0).original_topic);
        } finally {
            harness.close();
        }
    }

    @Example
    void staticBuildEnvelope_allFieldsPopulated() {
        ChangeEvent ev = event("orders", "u");
        DLQEnvelope env = DLQRouter.buildEnvelope(
                ev, ErrorType.SCHEMA_INCOMPATIBLE, "schema mismatch", 3, 42L);
        assertNotNull(env.original_topic);
        assertFalse(env.original_topic.isEmpty());
        assertEquals(3,    env.original_partition);
        assertEquals(42L,  env.original_offset);
        assertEquals(ErrorType.SCHEMA_INCOMPATIBLE, env.error_type);
        assertNotNull(env.error_message);
        assertTrue(env.error_timestamp_utc > 0);
        assertNotNull(env.raw_payload);
        assertTrue(env.raw_payload.length > 0);
    }

    @Example
    void staticBuildEnvelope_nullMessage_fallsBackToErrorTypeName() {
        ChangeEvent ev = event("customers", "c");
        DLQEnvelope env = DLQRouter.buildEnvelope(
                ev, ErrorType.DESERIALIZATION_FAILURE, null, 0, 0L);
        assertNotNull(env.error_message);
        assertFalse(env.error_message.isEmpty());
    }

    // =========================================================================
    // Generators
    // =========================================================================

    @Provide
    Arbitrary<ErrorType> errorTypes() {
        return Arbitraries.of(ErrorType.values());
    }

    @Provide
    Arbitrary<String> tableNames() {
        return Arbitraries.of("orders", "customers", "products");
    }

    @Provide
    Arbitrary<String> opCodes() {
        return Arbitraries.of("c", "u", "d", "r");
    }
}
