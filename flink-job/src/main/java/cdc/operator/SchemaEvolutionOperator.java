package cdc.operator;

import cdc.model.ChangeEvent;
import cdc.model.DLQEnvelope;
import cdc.model.ErrorType;
import cdc.schema.IcebergSchemaManager;
import cdc.schema.IncompatibleTypeChangeException;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink {@link RichMapFunction} that reconciles the schema of incoming
 * {@link ChangeEvent}s against the target Iceberg table, applying safe
 * schema evolution operations and routing incompatible changes to the DLQ.
 *
 * <h2>Schema evolution rules (design §6.6 / Requirements 7.1–7.4)</h2>
 * <ul>
 *   <li><b>New column</b> (Property 6): added to Iceberg with {@code NULL} default</li>
 *   <li><b>Dropped column</b> (Property 7): Iceberg retains it; new rows carry {@code NULL}</li>
 *   <li><b>Type widening</b> (Property 8): INT→LONG, FLOAT→DOUBLE, DECIMAL precision
 *       increase — applied automatically</li>
 *   <li><b>Incompatible type change</b> (Property 9): event routed to DLQ with
 *       {@code error_type = TYPE_MISMATCH}; alert logged; processing continues</li>
 * </ul>
 *
 * <h2>DLQ side output</h2>
 * <p>Because {@code RichMapFunction} does not support side outputs natively, this
 * operator exposes a {@link #takePendingDlqEnvelope()} accessor similar to
 * {@link AvroDeserializer}.  The wrapping job topology (see {@code CdcPipelineJob})
 * must call this method after each element and route non-null envelopes to the
 * DLQ Kafka sink via {@link ProcessFunction#Context#output}.
 *
 * <p>Alternatively, the job can wrap this operator in a {@link ProcessFunction}
 * that uses {@link #DLQ_OUTPUT_TAG} as the side-output channel.
 *
 * <h2>Thread safety</h2>
 * <p>Each Flink task-manager sub-task receives its own operator instance, so
 * no cross-thread synchronisation is needed.
 *
 * <p>Requirements: 7.1, 7.2, 7.3, 7.4
 */
public class SchemaEvolutionOperator
        extends RichMapFunction<ChangeEvent, ChangeEvent>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolutionOperator.class);

    // -------------------------------------------------------------------------
    // Public constants
    // -------------------------------------------------------------------------

    /**
     * Side-output tag for {@link DLQEnvelope} messages arising from
     * incompatible type changes.  Reference this tag in the job wiring.
     */
    public static final OutputTag<DLQEnvelope> DLQ_OUTPUT_TAG =
            new OutputTag<DLQEnvelope>("dlq-schema-evolution-errors") {};

    // -------------------------------------------------------------------------
    // Configuration (serializable)
    // -------------------------------------------------------------------------

    /** Base URL of the Iceberg REST catalog (e.g. {@code "http://iceberg-rest:8181"}). */
    private final String catalogUri;

    /** Object-store warehouse path (e.g. {@code "s3://warehouse/"}). */
    private final String warehouseUri;

    /** Iceberg namespace where CDC tables live (e.g. {@code "cdc"}). */
    private final String namespace;

    // -------------------------------------------------------------------------
    // Runtime state (transient — re-created per task-manager JVM)
    // -------------------------------------------------------------------------

    /**
     * Schema manager; either injected via {@link #setSchemaManager} (for testing)
     * or created from the catalog URI in {@link #open}.
     */
    private transient IcebergSchemaManager schemaManager;

    /**
     * Holds the most recently generated DLQ envelope so the wrapping operator
     * can retrieve and forward it to the side output.
     */
    private transient DLQEnvelope pendingDlqEnvelope;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Production constructor — the catalog connection is established in
     * {@link #open(Configuration)} from the supplied URIs.
     *
     * @param catalogUri   Iceberg REST catalog base URL
     * @param warehouseUri S3/MinIO warehouse path
     * @param namespace    Iceberg namespace for CDC tables
     */
    public SchemaEvolutionOperator(String catalogUri,
                                   String warehouseUri,
                                   String namespace) {
        this.catalogUri   = catalogUri;
        this.warehouseUri = warehouseUri;
        this.namespace    = namespace;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Opens the Iceberg REST catalog connection and initialises the
     * {@link IcebergSchemaManager}.
     *
     * <p>If a schema manager was already injected via {@link #setSchemaManager}
     * (e.g. in unit tests), this method skips catalog initialisation.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (schemaManager == null) {
            schemaManager = new IcebergSchemaManager(namespace);
            schemaManager.setCatalog(buildCatalog());
            LOG.info("SchemaEvolutionOperator opened — catalog={}, warehouse={}, namespace={}",
                    catalogUri, warehouseUri, namespace);
        }
    }

    // -------------------------------------------------------------------------
    // Processing
    // -------------------------------------------------------------------------

    /**
     * Inspects the column schema of the incoming {@link ChangeEvent} and applies
     * any safe evolution operations to the target Iceberg table.
     *
     * <p>On {@link IncompatibleTypeChangeException}: clears the event (returns
     * {@code null}), populates {@link #pendingDlqEnvelope}, and logs an alert.
     * The caller is responsible for routing null returns to DLQ.
     *
     * @param event the incoming Change Event
     * @return the original event (pass-through) on success, or {@code null} if
     *         the event was routed to DLQ
     */
    @Override
    public ChangeEvent map(ChangeEvent event) throws Exception {
        pendingDlqEnvelope = null;

        if (event.source == null) {
            // Malformed event without source metadata; pass through unchanged.
            return event;
        }

        String tableName = event.source.table;

        // Derive the incoming column schema from the row image.
        Map<String, Type> incomingFields = extractFieldTypes(event);
        if (incomingFields.isEmpty()) {
            // DELETE with no after-image and unknown before-image — pass through.
            return event;
        }

        try {
            schemaManager.applySchemaChange(tableName, incomingFields);
        } catch (IncompatibleTypeChangeException e) {
            LOG.error("[ALERT] Incompatible type change in table '{}': {} — routing to DLQ",
                    tableName, e.getMessage());
            pendingDlqEnvelope = buildDlqEnvelope(event, e);
            return null; // Caller must check and handle null
        } catch (Exception e) {
            // Unexpected catalog error — treat as processing error and route to DLQ.
            LOG.error("Unexpected error applying schema change for table '{}': {}",
                    tableName, e.getMessage(), e);
            pendingDlqEnvelope = buildDlqEnvelopeGeneric(event, e);
            return null;
        }

        return event;
    }

    // -------------------------------------------------------------------------
    // DLQ envelope hand-off
    // -------------------------------------------------------------------------

    /**
     * Returns and clears the pending {@link DLQEnvelope} produced during the
     * last {@link #map} invocation.
     *
     * <p>Returns {@code null} if the last call succeeded without DLQ routing.
     *
     * @return the pending envelope, or {@code null}
     */
    public DLQEnvelope takePendingDlqEnvelope() {
        DLQEnvelope envelope = pendingDlqEnvelope;
        pendingDlqEnvelope = null;
        return envelope;
    }

    // -------------------------------------------------------------------------
    // Test injection point
    // -------------------------------------------------------------------------

    /**
     * Injects a pre-configured {@link IcebergSchemaManager} for unit testing.
     * When this is set before {@link #open} is called, the operator skips
     * catalog initialisation.
     *
     * @param schemaManager the schema manager to use
     */
    public void setSchemaManager(IcebergSchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    /**
     * Returns the active schema manager (for test assertions).
     */
    public IcebergSchemaManager getSchemaManager() {
        return schemaManager;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Extracts column name → Iceberg {@link Type} from the most informative
     * image available in the event (after → before → empty).
     *
     * <p>Java object types are mapped to Iceberg primitive types using a
     * best-effort heuristic suitable for Debezium-decoded Avro values.
     */
    private static Map<String, Type> extractFieldTypes(ChangeEvent event) {
        Map<String, Object> image = (event.after != null) ? event.after : event.before;
        if (image == null) {
            return new HashMap<>();
        }

        Map<String, Type> fields = new HashMap<>();
        for (Map.Entry<String, Object> entry : image.entrySet()) {
            Type icebergType = javaToIcebergType(entry.getValue());
            if (icebergType != null) {
                fields.put(entry.getKey(), icebergType);
            }
        }
        return fields;
    }

    /**
     * Maps a Java value (as decoded by Avro) to a best-effort Iceberg
     * primitive type.  Returns {@code null} for {@code null} values (type
     * cannot be inferred) and for unsupported complex types.
     */
    static Type javaToIcebergType(Object value) {
        if (value == null)                  return null;
        if (value instanceof Integer)       return Types.IntegerType.get();
        if (value instanceof Long)          return Types.LongType.get();
        if (value instanceof Float)         return Types.FloatType.get();
        if (value instanceof Double)        return Types.DoubleType.get();
        if (value instanceof Boolean)       return Types.BooleanType.get();
        if (value instanceof String)        return Types.StringType.get();
        if (value instanceof byte[])        return Types.BinaryType.get();
        if (value instanceof java.math.BigDecimal) {
            java.math.BigDecimal bd = (java.math.BigDecimal) value;
            return Types.DecimalType.of(bd.precision(), bd.scale());
        }
        return Types.StringType.get(); // fallback for unrecognised types
    }

    /** Builds an Iceberg REST catalog client from the configured URIs. */
    private Catalog buildCatalog() {
        RESTCatalog restCatalog = new RESTCatalog();
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, catalogUri);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseUri);
        restCatalog.initialize("iceberg-rest", props);
        return restCatalog;
    }

    /** Builds a {@link DLQEnvelope} for an {@link IncompatibleTypeChangeException}. */
    private static DLQEnvelope buildDlqEnvelope(ChangeEvent event,
                                                 IncompatibleTypeChangeException e) {
        return new DLQEnvelope(
                topicOf(event),
                0,   // partition unknown at this stage — set by wrapping operator
                0L,  // offset unknown at this stage
                ErrorType.TYPE_MISMATCH,
                e.getMessage(),
                System.currentTimeMillis(),
                serializeEvent(event));
    }

    /** Builds a {@link DLQEnvelope} for a generic (non-type-change) exception. */
    private static DLQEnvelope buildDlqEnvelopeGeneric(ChangeEvent event, Exception e) {
        return new DLQEnvelope(
                topicOf(event),
                0,
                0L,
                ErrorType.PROCESSING_ERROR,
                e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(),
                System.currentTimeMillis(),
                serializeEvent(event));
    }

    private static String topicOf(ChangeEvent event) {
        if (event.source == null) return "unknown";
        return event.source.name + "." + event.source.schema + "." + event.source.table;
    }

    private static byte[] serializeEvent(ChangeEvent event) {
        return event.toString().getBytes(StandardCharsets.UTF_8);
    }
}
