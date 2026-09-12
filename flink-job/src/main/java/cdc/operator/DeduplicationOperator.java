package cdc.operator;

import cdc.model.ChangeEvent;
import cdc.model.PrimaryKey;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink {@link KeyedProcessFunction} that deduplicates Change Events within a
 * checkpoint window by keeping only the highest-LSN event per primary key.
 *
 * <h2>Design (§2.5 / §6.6)</h2>
 * <p>Debezium can re-deliver the same WAL event after a connector or Kafka
 * Connect restart.  The Flink job may also replay events from the last
 * checkpoint offset.  Both scenarios produce duplicate events for the same
 * primary key.
 *
 * <p>The operator maintains a {@link ValueState}&lt;Long&gt; that stores the
 * WAL LSN of the last event it forwarded for each keyed partition.  An
 * incoming event is emitted downstream <em>only</em> if its
 * {@code source.lsn > lastSeenLsn}.  After emitting, the stored LSN is
 * updated.
 *
 * <h2>State TTL / timer</h2>
 * <p>Every time an event is accepted, the operator registers an event-time
 * timer set {@code ttlMs} milliseconds ahead of the event's {@code source.ts_ms}.
 * When the timer fires in {@link #onTimer}, the state for that key is cleared,
 * preventing unbounded state growth for PKs that are never touched again.
 *
 * <h2>NULL LSN handling</h2>
 * <p>{@code source.lsn} is nullable (e.g. snapshot events may omit it).
 * A {@code null} LSN is treated as {@code Long.MIN_VALUE}: it will always be
 * superseded by any real LSN, but will still be emitted once if no prior
 * state exists for that key.
 *
 * <h2>Requirements</h2>
 * Requirements 5.3, 5.4 — within-batch deduplication preserves the latest
 * event (Property 4).
 */
public class DeduplicationOperator
        extends KeyedProcessFunction<PrimaryKey, ChangeEvent, ChangeEvent> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DeduplicationOperator.class);

    /**
     * Default TTL for per-key deduplication state: 24 hours in milliseconds.
     * Keys not seen within this window have their state cleared by the
     * event-time timer registered in {@link #processElement}.
     */
    public static final long DEFAULT_TTL_MS = 24L * 60 * 60 * 1_000; // 24 h

    /** TTL used for timer registration, configurable via constructor. */
    private final long ttlMs;

    /** Flink managed state: last-seen LSN per primary key. */
    private transient ValueState<Long> lastSeenLsnState;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Create operator with the default 24-hour TTL. */
    public DeduplicationOperator() {
        this(DEFAULT_TTL_MS);
    }

    /**
     * Create operator with a custom state TTL.
     *
     * @param ttlMs state TTL in milliseconds; must be positive
     */
    public DeduplicationOperator(long ttlMs) {
        if (ttlMs <= 0) {
            throw new IllegalArgumentException("ttlMs must be positive, got: " + ttlMs);
        }
        this.ttlMs = ttlMs;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("lastSeenLsn", Types.LONG);
        lastSeenLsnState = getRuntimeContext().getState(descriptor);
    }

    // -------------------------------------------------------------------------
    // Processing
    // -------------------------------------------------------------------------

    /**
     * Emits {@code event} downstream only when its LSN is strictly greater
     * than the last-seen LSN stored in state for this key.
     *
     * <p>After a successful emit the stored LSN is updated and an event-time
     * timer is scheduled to clear the state after {@link #ttlMs} ms.
     *
     * @param event    the incoming Change Event (keyed by {@link PrimaryKey})
     * @param ctx      Flink process-function context
     * @param out      output collector
     * @throws Exception if state access fails
     */
    @Override
    public void processElement(ChangeEvent event,
                               Context ctx,
                               Collector<ChangeEvent> out) throws Exception {

        // Treat a null LSN as the smallest possible value so that it is
        // always superseded by a real LSN but is still forwarded once
        // if no state exists yet.
        long incomingLsn = (event.source != null && event.source.lsn != null)
                ? event.source.lsn
                : Long.MIN_VALUE;

        Long storedLsn = lastSeenLsnState.value();

        if (storedLsn == null || incomingLsn > storedLsn) {
            // This is a new or later event — forward it.
            out.collect(event);
            lastSeenLsnState.update(incomingLsn);

            // Schedule a cleanup timer in event time.  If event.source.ts_ms
            // is null or zero fall back to processing time as a best-effort.
            long eventTimeMs = (event.source != null && event.source.ts_ms > 0)
                    ? event.source.ts_ms
                    : ctx.timerService().currentProcessingTime();

            ctx.timerService().registerEventTimeTimer(eventTimeMs + ttlMs);

            LOG.debug("Forwarded event for key {} with LSN {} (previous: {})",
                    ctx.getCurrentKey(), incomingLsn, storedLsn);
        } else {
            LOG.debug("Dropped duplicate event for key {} with LSN {} <= stored LSN {}",
                    ctx.getCurrentKey(), incomingLsn, storedLsn);
        }
    }

    /**
     * Timer callback: clears per-key state once the TTL window has elapsed.
     * This prevents unbounded state growth for keys that are no longer active.
     *
     * @param timestamp the timer fire time
     * @param ctx       timer context
     * @param out       output collector (unused)
     * @throws Exception if state clear fails
     */
    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<ChangeEvent> out) throws Exception {
        LOG.debug("TTL timer fired for key {} at timestamp {} — clearing LSN state",
                ctx.getCurrentKey(), timestamp);
        lastSeenLsnState.clear();
    }
}
