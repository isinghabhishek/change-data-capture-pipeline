package cdc.operator;

import cdc.model.ChangeEvent;
import cdc.model.DLQEnvelope;
import cdc.model.ErrorType;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Flink {@link ProcessFunction} that routes {@link ChangeEvent}s to either
 * the main output (success path) or to the DLQ side output (error path).
 *
 * <h2>Responsibilities (Requirements 8.1, 8.2)</h2>
 * <p>The {@code DLQRouter} is the final gate before events leave the Flink
 * operator graph and reach the {@code IcebergSink}.  Any event that arrives
 * already marked as an error (via the injected {@link ErrorContext}) is
 * packaged into a fully-populated {@link DLQEnvelope} and emitted to
 * {@link #DLQ_OUTPUT_TAG}; all other events pass straight to the main output.
 *
 * <h2>DLQ envelope population (Property 10)</h2>
 * All seven required fields are populated on every DLQ-routed event:
 * <ol>
 *   <li>{@code original_topic} — derived from {@code event.source}</li>
 *   <li>{@code original_partition} — from the Kafka record metadata carried by {@link ErrorContext}</li>
 *   <li>{@code original_offset} — from the Kafka record metadata</li>
 *   <li>{@code error_type} — from {@link ErrorContext}</li>
 *   <li>{@code error_message} — from {@link ErrorContext}</li>
 *   <li>{@code error_timestamp_utc} — current wall-clock time in ms</li>
 *   <li>{@code raw_payload} — UTF-8 serialisation of the {@link ChangeEvent}</li>
 * </ol>
 *
 * <h2>Usage</h2>
 * <p>Wrap any stream segment where errors can occur in a {@code DLQRouter}
 * and attach the side output to a Kafka DLQ producer sink:
 * <pre>{@code
 * SingleOutputStreamOperator<ChangeEvent> routed =
 *     stream.process(new DLQRouter());
 * routed.getSideOutput(DLQRouter.DLQ_OUTPUT_TAG)
 *       .addSink(kafkaDlqSink);
 * }</pre>
 *
 * <p>Requirements: 8.1, 8.2
 */
public class DLQRouter
        extends ProcessFunction<ChangeEvent, ChangeEvent>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DLQRouter.class);

    // -------------------------------------------------------------------------
    // Side-output tag
    // -------------------------------------------------------------------------

    /**
     * Side-output tag for {@link DLQEnvelope} messages.
     * Reference this in the job wiring to attach a Kafka DLQ sink.
     */
    public static final OutputTag<DLQEnvelope> DLQ_OUTPUT_TAG =
            new OutputTag<DLQEnvelope>("dlq-router-errors") {};

    // -------------------------------------------------------------------------
    // ErrorContext — carries per-record error metadata through the stream
    // -------------------------------------------------------------------------

    /**
     * Lightweight container for the error metadata attached to a
     * {@link ChangeEvent} that must be routed to the DLQ.
     *
     * <p>Instances are created by upstream operators (e.g.
     * {@link SchemaEvolutionOperator}) and looked up in {@link DLQRouter}
     * by the event's identity.  In practice the router inspects the
     * {@link #pendingError} field injected before calling
     * {@link DLQRouter#processElement}.
     */
    public static final class ErrorContext implements Serializable {

        private static final long serialVersionUID = 1L;

        public final ErrorType  errorType;
        public final String     errorMessage;
        public final int        partition;
        public final long       offset;

        public ErrorContext(ErrorType errorType, String errorMessage,
                            int partition, long offset) {
            this.errorType    = errorType;
            this.errorMessage = errorMessage;
            this.partition    = partition;
            this.offset       = offset;
        }
    }

    // -------------------------------------------------------------------------
    // State: pending error injected before processElement
    // -------------------------------------------------------------------------

    /**
     * If non-null when {@link #processElement} is called, the event is routed
     * to the DLQ using this context.  Cleared after each invocation.
     *
     * <p>Set this field on the operator instance before pushing the event
     * through the test harness (or via the job wiring helper
     * {@link #routeToDlq}).
     */
    public transient ErrorContext pendingError;

    // -------------------------------------------------------------------------
    // Processing
    // -------------------------------------------------------------------------

    /**
     * Routes {@code event} to either the main output or DLQ side output.
     *
     * <p>If {@link #pendingError} is set the event is dead-lettered;
     * otherwise it is forwarded to the main output unchanged.
     *
     * @param event the incoming Change Event
     * @param ctx   process-function context (provides side-output access)
     * @param out   main output collector
     */
    @Override
    public void processElement(ChangeEvent event,
                               Context ctx,
                               Collector<ChangeEvent> out) {

        if (pendingError != null) {
            ErrorContext err = pendingError;
            pendingError = null;

            DLQEnvelope envelope = buildEnvelope(event, err);
            ctx.output(DLQ_OUTPUT_TAG, envelope);

            LOG.warn("Event routed to DLQ [topic={}, partition={}, offset={}] error_type={}: {}",
                    envelope.original_topic, envelope.original_partition,
                    envelope.original_offset, err.errorType, err.errorMessage);
        } else {
            out.collect(event);
        }
    }

    // -------------------------------------------------------------------------
    // Convenience factory — used in CdcPipelineJob wiring
    // -------------------------------------------------------------------------

    /**
     * Creates a fully-populated {@link DLQEnvelope} for an event that failed
     * processing.  Can be called from job wiring code that does not go through
     * the Flink operator lifecycle (e.g. side-output forwarding from a
     * {@link org.apache.flink.api.common.functions.RichMapFunction}).
     *
     * @param event        the Change Event that failed
     * @param errorType    error classification
     * @param errorMessage human-readable description
     * @param partition    Kafka partition of the original record (0 if unknown)
     * @param offset       Kafka offset of the original record (0 if unknown)
     * @return a complete {@link DLQEnvelope} with all 7 fields populated
     */
    public static DLQEnvelope buildEnvelope(ChangeEvent event,
                                            ErrorType errorType,
                                            String errorMessage,
                                            int partition,
                                            long offset) {
        return new DLQEnvelope(
                topicOf(event),
                partition,
                offset,
                errorType,
                errorMessage != null ? errorMessage : errorType.name(),
                System.currentTimeMillis(),
                serializeEvent(event));
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static DLQEnvelope buildEnvelope(ChangeEvent event, ErrorContext err) {
        return buildEnvelope(event, err.errorType, err.errorMessage,
                err.partition, err.offset);
    }

    private static String topicOf(ChangeEvent event) {
        if (event == null || event.source == null) return "unknown";
        return event.source.name + "." + event.source.schema + "." + event.source.table;
    }

    private static byte[] serializeEvent(ChangeEvent event) {
        if (event == null) return new byte[0];
        return event.toString().getBytes(StandardCharsets.UTF_8);
    }
}
