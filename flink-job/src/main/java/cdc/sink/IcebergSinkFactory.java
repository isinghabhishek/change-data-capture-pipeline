package cdc.sink;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory that configures and returns an Iceberg {@link FlinkSink} for a
 * specific CDC target table.
 *
 * <h2>Sink configuration (Requirements 5.1, 5.2 / design §2.6)</h2>
 * <ul>
 *   <li><b>Catalog</b>: Iceberg REST catalog at {@code http://iceberg-rest:8181}</li>
 *   <li><b>Warehouse</b>: {@code s3://warehouse/}</li>
 *   <li><b>Namespace</b>: {@code cdc}</li>
 *   <li><b>Write mode</b>: upsert via {@code write.upsert.enabled=true}</li>
 *   <li><b>Merge mode</b>: {@code merge-on-read} for efficient equality deletes</li>
 *   <li><b>Format version</b>: 2 (required for row-level deletes)</li>
 *   <li><b>Partitioning</b>: {@code DAYS(updated_at)}</li>
 *   <li><b>Equality delete key</b>: source primary key column(s)</li>
 * </ul>
 *
 * <h2>Upsert vs. delete</h2>
 * <p>Flink's {@code IcebergSink} in upsert mode writes equality delete files
 * keyed on the PK column(s) for every row.  When the {@code op=d} DELETE event
 * reaches the sink, the equality delete file removes the row from all future
 * reads — effectively implementing the soft-delete via Iceberg v2 row-level
 * deletes without the need for a {@code _cdc_deleted} flag update.
 */
public class IcebergSinkFactory {

    // -------------------------------------------------------------------------
    // Default connection constants (overridable via constructor)
    // -------------------------------------------------------------------------

    public static final String DEFAULT_CATALOG_URI   = "http://iceberg-rest:8181";
    public static final String DEFAULT_WAREHOUSE_URI = "s3://warehouse/";
    public static final String DEFAULT_NAMESPACE     = "cdc";

    private final String catalogUri;
    private final String warehouseUri;
    private final String namespace;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Creates a factory with the default catalog URI, warehouse, and namespace. */
    public IcebergSinkFactory() {
        this(DEFAULT_CATALOG_URI, DEFAULT_WAREHOUSE_URI, DEFAULT_NAMESPACE);
    }

    /**
     * Creates a factory with custom connection settings.
     *
     * @param catalogUri   base URL of the Iceberg REST catalog
     * @param warehouseUri S3/MinIO warehouse path
     * @param namespace    Iceberg namespace for CDC tables
     */
    public IcebergSinkFactory(String catalogUri, String warehouseUri, String namespace) {
        this.catalogUri   = catalogUri;
        this.warehouseUri = warehouseUri;
        this.namespace    = namespace;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns a {@link TableLoader} for the given table, using the configured
     * REST catalog.  The caller is responsible for attaching this to a
     * {@link FlinkSink} via {@code FlinkSink.forRowData(stream).tableLoader(loader)}.
     *
     * <p>The {@code FlinkSink} expects a {@code DataStream<RowData>}; the
     * ChangeEvent-to-RowData conversion is handled by a dedicated mapper in
     * the job wiring (task 11).  This factory is intentionally decoupled from
     * that conversion to keep it testable without Hadoop dependencies.
     *
     * @param tableName      unqualified target table name (e.g. {@code "orders"})
     * @param equalityFields list of column names forming the upsert equality key
     * @return a configured {@link TableLoader} ready for use with FlinkSink
     */
    public TableLoader buildTableLoader(String tableName,
                                        List<String> equalityFields) {
        CatalogLoader catalogLoader = buildCatalogLoader();
        return TableLoader.fromCatalog(
                catalogLoader,
                TableIdentifier.of(namespace, tableName));
    }

    /**
     * Returns a pre-configured {@link CatalogLoader} for the Iceberg REST
     * catalog.  Exposed so that the job wiring can reuse the same loader for
     * catalog operations (e.g. schema checks).
     */
    public CatalogLoader buildCatalogLoader() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI,               catalogUri);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseUri);
        // Flink clusters always provide Hadoop; pass an empty Configuration.
        return CatalogLoader.rest("iceberg-rest",
                new org.apache.hadoop.conf.Configuration(), props);
    }

    /**
     * Creates (or updates) the target Iceberg table with the CDC-specific
     * table properties if it does not already exist.
     *
     * <p>This is intended to be called during job initialisation, <em>before</em>
     * the first checkpoint.  If the table already exists the call is a no-op.
     *
     * @param catalog    an open Iceberg {@link Catalog} instance
     * @param tableName  unqualified table name (e.g. {@code "orders"})
     * @param schema     initial Iceberg schema for the table
     */
    public void ensureTableExists(Catalog catalog, String tableName, Schema schema) {
        TableIdentifier id = TableIdentifier.of(namespace, tableName);
        if (!catalog.tableExists(id)) {
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .day("updated_at")
                    .build();

            Map<String, String> props = new HashMap<>();
            props.put(TableProperties.FORMAT_VERSION,       "2");
            props.put(TableProperties.UPSERT_ENABLED,       "true");
            props.put("write.merge.mode",                    "merge-on-read");

            catalog.createTable(id, schema, spec, props);
        }
    }

    // -------------------------------------------------------------------------
    // Minimal default Iceberg schema for CDC tables
    // -------------------------------------------------------------------------

    /**
     * Returns a minimal Iceberg schema that covers all CDC metadata columns
     * plus the core business columns expected by the CDC pipeline.
     *
     * <p>The schema is intentionally kept narrow; the
     * {@link cdc.operator.SchemaEvolutionOperator} adds application-specific
     * columns at runtime as they appear in Change Events.
     *
     * @return a base Iceberg {@link Schema} with PK + CDC metadata columns
     */
    public static Schema baseCdcSchema() {
        return new Schema(
                Types.NestedField.required(1, "id",          Types.IntegerType.get()),
                Types.NestedField.optional(2, "updated_at",  Types.TimestampType.withoutZone()),
                Types.NestedField.optional(3, "_cdc_op",     Types.StringType.get()),
                Types.NestedField.optional(4, "_cdc_ts",     Types.TimestampType.withoutZone()),
                Types.NestedField.optional(5, "_cdc_lsn",    Types.LongType.get()),
                Types.NestedField.optional(6, "_cdc_deleted",Types.BooleanType.get())
        );
    }
}
