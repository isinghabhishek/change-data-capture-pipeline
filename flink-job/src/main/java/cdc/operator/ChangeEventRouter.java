package cdc.operator;

import cdc.model.ChangeEvent;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink {@link ProcessFunction} that fans out a unified multi-topic
 * {@link ChangeEvent} stream into per-table side outputs.
 *
 * <h2>Design (§2.5)</h2>
 * <p>The {@code KafkaSource} consumes all three CDC topics in a single source
 * operator.  This router inspects {@code event.source.table} and emits each
 * event to the corresponding per-table {@link OutputTag}.  Events for unknown
 * tables are forwarded to the main output (pass-through) so they are not
 * silently dropped.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * SingleOutputStreamOperator<ChangeEvent> routed =
 *     allEvents.process(new ChangeEventRouter());
 *
 * DataStream<ChangeEvent> orders =
 *     routed.getSideOutput(ChangeEventRouter.ordersTag());
 * DataStream<ChangeEvent> customers =
 *     routed.getSideOutput(ChangeEventRouter.customersTag());
 * }</pre>
 */
public class ChangeEventRouter
        extends ProcessFunction<ChangeEvent, ChangeEvent>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    // -------------------------------------------------------------------------
    // Per-table output tags (public so CdcPipelineJob can reference them)
    // -------------------------------------------------------------------------

    /** Side-output tag for the {@code orders} table stream. */
    public static final OutputTag<ChangeEvent> ORDERS_TAG =
            new OutputTag<ChangeEvent>("orders") {};

    /** Side-output tag for the {@code customers} table stream. */
    public static final OutputTag<ChangeEvent> CUSTOMERS_TAG =
            new OutputTag<ChangeEvent>("customers") {};

    /** Side-output tag for the {@code products} table stream. */
    public static final OutputTag<ChangeEvent> PRODUCTS_TAG =
            new OutputTag<ChangeEvent>("products") {};

    // -------------------------------------------------------------------------
    // Routing table (built lazily — maps table name -> output tag)
    // -------------------------------------------------------------------------

    private static final Map<String, OutputTag<ChangeEvent>> ROUTING_TABLE;
    static {
        ROUTING_TABLE = new HashMap<>();
        ROUTING_TABLE.put("orders",    ORDERS_TAG);
        ROUTING_TABLE.put("customers", CUSTOMERS_TAG);
        ROUTING_TABLE.put("products",  PRODUCTS_TAG);
    }

    /**
     * Returns the side-output tag for the given source table name,
     * or {@code null} if the table is unknown.
     */
    public static OutputTag<ChangeEvent> tagForTable(String tableName) {
        return ROUTING_TABLE.get(tableName);
    }

    // -------------------------------------------------------------------------
    // Processing
    // -------------------------------------------------------------------------

    @Override
    public void processElement(ChangeEvent event,
                               Context ctx,
                               Collector<ChangeEvent> out) {

        if (event.source == null) {
            out.collect(event); // pass through unknown events
            return;
        }

        OutputTag<ChangeEvent> tag = ROUTING_TABLE.get(event.source.table);
        if (tag != null) {
            ctx.output(tag, event);
        } else {
            // Unknown table — emit to main output; will be visible to operators
            // that consume the main stream, but won't reach per-table pipelines.
            out.collect(event);
        }
    }
}
