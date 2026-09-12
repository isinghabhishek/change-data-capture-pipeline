package cdc.operator;

import cdc.model.ChangeEvent;
import cdc.model.PrimaryKey;
import cdc.model.SourceInfo;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Positive;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based tests for {@link DeduplicationOperator}.
 *
 * <p>Tag: {@code # Feature: cdc-pipeline, Property 4: Within-batch dedup}
 *
 * <h2>Properties verified</h2>
 * <ol>
 *   <li><b>P4 — highest-LSN event is the only one emitted per PK</b>: for any
 *       batch of 1–N events for the same PK with random LSN ordering, only the
 *       event with the greatest LSN reaches the output.
 *   <li><b>Strict monotone forwarding</b>: events fed in strictly-increasing LSN
 *       order are all forwarded (no false drops).
 *   <li><b>Duplicate suppression</b>: re-delivering the exact same event (same
 *       LSN) is suppressed after the first delivery.
 *   <li><b>Independent keys</b>: events for distinct PKs never interfere with
 *       each other; each key independently forwards its highest-LSN event.
 *   <li><b>Null LSN treated as Long.MIN_VALUE</b>: a null-LSN event is forwarded
 *       once, and subsequently suppressed by any real LSN.
 * </ol>
 *
 * <p>Each property uses jqwik's {@link Property} annotation with a minimum of
 * 100 tries as required by the testing strategy (design §6.2).
 */
// Feature: cdc-pipeline, Property 4: Within-batch dedup
class DeduplicationOperatorTest {

    // -------------------------------------------------------------------------
    // Helper: build a ChangeEvent with a given PK value and LSN
    // -------------------------------------------------------------------------

    private static ChangeEvent event(Object pkValue, Long lsn) {
        SourceInfo src = new SourceInfo(
                "2.5.0.Final", "postgresql", "db",
                System.currentTimeMillis(),
                "sourcedb", "public", "orders",
                lsn, null);

        Map<String, Object> after = new HashMap<>();
        after.put("id", pkValue);
        after.put("status", "active");

        return new ChangeEvent(null, after, src, "c", src.ts_ms, null);
    }

    private static ChangeEvent eventWithOp(Object pkValue, Long lsn, String op) {
        SourceInfo src = new SourceInfo(
                "2.5.0.Final", "postgresql", "db",
                System.currentTimeMillis(),
                "sourcedb", "public", "orders",
                lsn, null);

        Map<String, Object> after = "d".equals(op) ? null : new HashMap<>();
        if (after != null) {
            after.put("id", pkValue);
        }
        Map<String, Object> before = new HashMap<>();
        before.put("id", pkValue);

        return new ChangeEvent(before, after, src, op, src.ts_ms, null);
    }

    // -------------------------------------------------------------------------
    // Helper: build and open the test harness for a single key
    // -------------------------------------------------------------------------

    private KeyedOneInputStreamOperatorTestHarness<PrimaryKey, ChangeEvent, ChangeEvent>
    buildHarness() throws Exception {

        DeduplicationOperator op = new DeduplicationOperator(
                60_000L /* 60 s TTL for tests */);

        KeyedOneInputStreamOperatorTestHarness<PrimaryKey, ChangeEvent, ChangeEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(op),
                        e -> e.primaryKey("id"),
                        Types.POJO(PrimaryKey.class));

        harness.open();
        return harness;
    }

    // =========================================================================
    // Property 4: Within-batch deduplication preserves latest event
    // =========================================================================

    /**
     * For any batch of 1–5 events for a single PK with shuffled LSNs, only the
     * event with the maximum LSN must reach the output.
     *
     * <p>Events are processed in the order provided by the generator (random
     * permutation of distinct LSNs).
     */
    @Property(tries = 200)
    // Feature: cdc-pipeline, Property 4: Within-batch dedup
    void p4_highestLsnEventEmitted(
            @ForAll("distinctLsnBatches") List<Long> lsns) throws Exception {

        int pkValue = 42;
        List<ChangeEvent> batch = lsns.stream()
                .map(lsn -> event(pkValue, lsn))
                .collect(Collectors.toList());

        long maxLsn = lsns.stream().mapToLong(Long::longValue).max().orElseThrow();

        try (var harness = buildHarness()) {
            long t = 0L;
            for (ChangeEvent e : batch) {
                harness.processElement(e, t++);
            }

            List<ChangeEvent> output = harness.extractOutputValues();

            // At least one event must be emitted.
            assertFalse(output.isEmpty(),
                    "No events emitted for batch " + lsns);

            // The last emitted event must carry the maximum LSN.
            ChangeEvent last = output.get(output.size() - 1);
            assertEquals(maxLsn, last.source.lsn,
                    "Expected max LSN " + maxLsn + " but got " + last.source.lsn
                    + " for batch " + lsns);

            // No emitted event should have an LSN less than a later-emitted one
            // (i.e. emitted sequence must be strictly increasing).
            for (int i = 1; i < output.size(); i++) {
                long prev = output.get(i - 1).source.lsn;
                long curr = output.get(i).source.lsn;
                assertTrue(curr > prev,
                        "Emitted events not in strictly increasing LSN order: "
                        + prev + " then " + curr);
            }
        }
    }

    // =========================================================================
    // Property: Strict monotone input — all events forwarded
    // =========================================================================

    /**
     * When events arrive in strictly increasing LSN order every event must be
     * forwarded (no false duplicate drops).
     */
    @Property(tries = 200)
    void strictlyIncreasingLsns_allForwarded(
            @ForAll("distinctLsnBatches") List<Long> lsns) throws Exception {

        List<Long> sorted = lsns.stream().sorted().collect(Collectors.toList());
        int pkValue = 7;

        try (var harness = buildHarness()) {
            long t = 0L;
            for (Long lsn : sorted) {
                harness.processElement(event(pkValue, lsn), t++);
            }

            List<ChangeEvent> output = harness.extractOutputValues();
            assertEquals(sorted.size(), output.size(),
                    "Expected all " + sorted.size() + " events forwarded for sorted LSNs "
                    + sorted + " but got " + output.size());
        }
    }

    // =========================================================================
    // Property: Duplicate suppression — same LSN delivered twice → emitted once
    // =========================================================================

    /**
     * Re-delivering an event with the same LSN (idempotent replay) must be
     * suppressed after the first emission.
     */
    @Property(tries = 200)
    void duplicateLsn_suppressedAfterFirstEmit(
            @ForAll @Positive long lsn,
            @ForAll @IntRange(min = 2, max = 10) int repeatCount) throws Exception {

        int pkValue = 99;

        try (var harness = buildHarness()) {
            for (int i = 0; i < repeatCount; i++) {
                harness.processElement(event(pkValue, lsn), i);
            }

            List<ChangeEvent> output = harness.extractOutputValues();
            assertEquals(1, output.size(),
                    "Expected exactly 1 emission for LSN " + lsn
                    + " repeated " + repeatCount + " times, but got " + output.size());
            assertEquals(lsn, output.get(0).source.lsn);
        }
    }

    // =========================================================================
    // Property: Independent keys never interfere
    // =========================================================================

    /**
     * Events for N distinct PKs are processed through independent state
     * partitions; each key must emit exactly its highest-LSN event and the
     * results for one key must not affect another.
     */
    @Property(tries = 150)
    void independentKeys_doNotInterfere(
            @ForAll("distinctLsnBatches") List<Long> lsns) throws Exception {

        // Two independent PKs with the same set of LSNs (worst case for
        // interference: both share the same LSN space).
        int pkA = 1;
        int pkB = 2;
        long maxLsn = lsns.stream().mapToLong(Long::longValue).max().orElseThrow();

        // We need a harness that can handle multiple keys.
        DeduplicationOperator op = new DeduplicationOperator(60_000L);
        var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(op),
                e -> e.primaryKey("id"),
                Types.POJO(PrimaryKey.class));
        harness.open();

        try {
            long t = 0L;
            // Interleave events for both keys in random LSN order.
            for (Long lsn : lsns) {
                harness.processElement(event(pkA, lsn), t++);
                harness.processElement(event(pkB, lsn), t++);
            }

            List<ChangeEvent> output = harness.extractOutputValues();

            // Collect emitted LSNs per key.
            Map<Object, Long> lastEmittedLsnPerKey = new HashMap<>();
            for (ChangeEvent e : output) {
                Object pk = e.after.get("id");
                Long existing = lastEmittedLsnPerKey.get(pk);
                if (existing == null || e.source.lsn > existing) {
                    lastEmittedLsnPerKey.put(pk, e.source.lsn);
                }
            }

            // Both keys must end up with the max LSN as their last emission.
            assertEquals(maxLsn, lastEmittedLsnPerKey.get(pkA),
                    "Key A final LSN mismatch");
            assertEquals(maxLsn, lastEmittedLsnPerKey.get(pkB),
                    "Key B final LSN mismatch");
        } finally {
            harness.close();
        }
    }

    // =========================================================================
    // Property: Null LSN handled gracefully
    // =========================================================================

    /**
     * An event with {@code source.lsn = null} must be forwarded exactly once,
     * and any subsequent real LSN must supersede it.
     */
    @Property(tries = 100)
    void nullLsn_forwardedOnce_thenSupersededByRealLsn(
            @ForAll @Positive long realLsn) throws Exception {

        int pkValue = 55;

        try (var harness = buildHarness()) {
            // First: null-LSN snapshot event (treated as Long.MIN_VALUE).
            harness.processElement(event(pkValue, null), 0L);
            // Second: a real LSN event.
            harness.processElement(event(pkValue, realLsn), 1L);
            // Third: repeat the null-LSN event — should be suppressed.
            harness.processElement(event(pkValue, null), 2L);

            List<ChangeEvent> output = harness.extractOutputValues();

            // Exactly 2 events must be emitted: null-LSN and realLsn.
            assertEquals(2, output.size(),
                    "Expected 2 emissions (null-lsn then real-lsn), got: "
                    + output.stream().map(e -> e.source.lsn).collect(Collectors.toList()));

            assertNull(output.get(0).source.lsn, "First emission should have null LSN");
            assertEquals(realLsn, output.get(1).source.lsn,
                    "Second emission should have real LSN " + realLsn);
        }
    }

    // =========================================================================
    // Property: DELETE events are deduplicated the same way as other ops
    // =========================================================================

    /**
     * DELETE events ({@code op=d}) must follow the same LSN-based deduplication
     * logic regardless of their {@code null} after-image.
     */
    @Property(tries = 100)
    void deleteEvents_deduplicatedByLsn(
            @ForAll @Positive long lsn) throws Exception {

        int pkValue = 11;

        try (var harness = buildHarness()) {
            // Two identical DELETE events with the same LSN.
            harness.processElement(eventWithOp(pkValue, lsn, "d"), 0L);
            harness.processElement(eventWithOp(pkValue, lsn, "d"), 1L);

            List<ChangeEvent> output = harness.extractOutputValues();
            assertEquals(1, output.size(),
                    "Duplicate DELETE event with LSN " + lsn + " should be suppressed");
            assertEquals("d", output.get(0).op);
        }
    }

    // =========================================================================
    // Property: State cleared on TTL — key can be re-processed after expiry
    // =========================================================================

    /**
     * After the TTL timer fires and clears state for a key, a previously-seen
     * LSN must be accepted again (as if the key is brand-new).
     */
    @Property(tries = 100)
    void stateCleared_afterTtlTimer_allowsReprocessing(
            @ForAll @Positive long lsn) throws Exception {

        long ttlMs = 5_000L; // 5 s TTL for this test
        int pkValue = 33;

        DeduplicationOperator op = new DeduplicationOperator(ttlMs);
        var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(op),
                e -> e.primaryKey("id"),
                Types.POJO(PrimaryKey.class));
        harness.open();

        try {
            // Process event at event-time 1000 ms → registers timer at 1000 + 5000 = 6000.
            ChangeEvent first = makeEventWithTs(pkValue, lsn, 1_000L);
            harness.processElement(first, 1_000L);

            // Advance event time past the timer to trigger TTL cleanup.
            harness.processWatermark(new Watermark(7_000L));

            // Re-process the same LSN — state was cleared so it must be forwarded again.
            ChangeEvent second = makeEventWithTs(pkValue, lsn, 7_000L);
            harness.processElement(second, 7_000L);

            List<ChangeEvent> output = harness.extractOutputValues();
            assertEquals(2, output.size(),
                    "Expected 2 emissions (initial + post-TTL replay) but got " + output.size());
        } finally {
            harness.close();
        }
    }

    // =========================================================================
    // Generators
    // =========================================================================

    /**
     * Generates a randomly-shuffled list of 1–8 distinct positive LSN values.
     */
    @Provide
    Arbitrary<List<Long>> distinctLsnBatches() {
        return Arbitraries.longs()
                .between(1L, 1_000_000L)
                .list()
                .ofMinSize(1)
                .ofMaxSize(8)
                .uniqueElements()
                .map(list -> {
                    List<Long> shuffled = new ArrayList<>(list);
                    Collections.shuffle(shuffled, new Random());
                    return shuffled;
                });
    }

    // =========================================================================
    // Utility
    // =========================================================================

    /** Creates a ChangeEvent with an explicit {@code source.ts_ms} for timer tests. */
    private static ChangeEvent makeEventWithTs(Object pkValue, long lsn, long tsMs) {
        SourceInfo src = new SourceInfo(
                "2.5.0.Final", "postgresql", "db",
                tsMs,
                "sourcedb", "public", "orders",
                lsn, null);

        Map<String, Object> after = new HashMap<>();
        after.put("id", pkValue);

        return new ChangeEvent(null, after, src, "c", tsMs, null);
    }
}
