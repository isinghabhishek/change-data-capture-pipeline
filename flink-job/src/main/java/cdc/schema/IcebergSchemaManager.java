package cdc.schema;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Thin wrapper around the Iceberg {@link Catalog} that applies safe schema
 * evolution operations to a target table.
 *
 * <p>This class is intentionally separated from {@link cdc.operator.SchemaEvolutionOperator}
 * so that it can be replaced with a test-double (mock/stub) in unit tests without
 * requiring a live Iceberg REST catalog.
 *
 * <h2>Operations performed (design §6.6 schema evolution algorithm)</h2>
 * <ul>
 *   <li><b>New column</b>: added with {@code NULL} default (optional column)</li>
 *   <li><b>Dropped column</b>: no action — Iceberg retains it; new rows get {@code NULL}</li>
 *   <li><b>Type widening</b>: column type updated to the wider type (INT→LONG etc.)</li>
 *   <li><b>Incompatible type change</b>: {@link IncompatibleTypeChangeException} thrown</li>
 * </ul>
 *
 * <p>Requirements: 7.1, 7.2, 7.3, 7.4
 */
public class IcebergSchemaManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSchemaManager.class);

    /** Iceberg namespace where CDC tables reside (e.g. {@code "cdc"}). */
    private final String namespace;

    /**
     * Catalog instance (transient — re-created after serialisation).
     * Injected via {@link #setCatalog} in {@code open()} of the parent operator.
     */
    private transient Catalog catalog;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * @param namespace Iceberg namespace (e.g. {@code "cdc"})
     */
    public IcebergSchemaManager(String namespace) {
        this.namespace = namespace;
    }

    // -------------------------------------------------------------------------
    // Catalog injection (called in RichFunction.open())
    // -------------------------------------------------------------------------

    /** Injects the live catalog. Called from the operator's {@code open()} method. */
    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    /** @return the injected catalog (may be {@code null} before {@link #setCatalog} is called) */
    public Catalog getCatalog() {
        return catalog;
    }

    // -------------------------------------------------------------------------
    // Schema evolution
    // -------------------------------------------------------------------------

    /**
     * Reconciles the incoming column schema (extracted from the {@code after}
     * or {@code before} image of a {@link cdc.model.ChangeEvent}) against the
     * current Iceberg table schema, applying any safe evolution operations.
     *
     * <p>The method is idempotent: if the incoming schema already matches the
     * Iceberg schema exactly, no catalog call is made.
     *
     * @param tableName     the unqualified Iceberg table name (e.g. {@code "orders"})
     * @param incomingFields map of column-name → Iceberg {@link Type} derived from
     *                       the event's row image
     * @throws IncompatibleTypeChangeException if any column has an incompatible
     *                                          type change
     * @throws IllegalStateException           if the catalog has not been injected
     */
    public void applySchemaChange(String tableName,
                                  Map<String, Type> incomingFields)
            throws IncompatibleTypeChangeException {

        if (catalog == null) {
            throw new IllegalStateException(
                    "IcebergSchemaManager.catalog is null — call setCatalog() first");
        }

        TableIdentifier id    = TableIdentifier.of(namespace, tableName);
        Table           table = catalog.loadTable(id);
        Schema          current = table.schema();

        // Index current fields by name for O(1) look-up.
        Map<String, Types.NestedField> currentFields = current.columns().stream()
                .collect(Collectors.toMap(Types.NestedField::name, f -> f));

        // Detect whether any change is needed before starting an update transaction.
        boolean needsUpdate = false;
        for (Map.Entry<String, Type> entry : incomingFields.entrySet()) {
            String colName      = entry.getKey();
            Type   incomingType = entry.getValue();

            if (!currentFields.containsKey(colName)) {
                // New column — will be added.
                needsUpdate = true;
                break;
            }

            Type currentType = currentFields.get(colName).type();
            if (!currentType.equals(incomingType)) {
                if (TypeCompatibility.isTypeWidening(currentType, incomingType)) {
                    needsUpdate = true;
                    break;
                } else {
                    // Will throw — validate all columns first so the exception
                    // message names the first incompatible column encountered.
                    throw new IncompatibleTypeChangeException(colName, currentType, incomingType);
                }
            }
        }

        if (!needsUpdate) {
            return; // Schema already up-to-date.
        }

        // Apply changes in a single update transaction.
        var updateSchema = table.updateSchema();

        for (Map.Entry<String, Type> entry : incomingFields.entrySet()) {
            String colName      = entry.getKey();
            Type   incomingType = entry.getValue();

            if (!currentFields.containsKey(colName)) {
                LOG.info("Schema evolution [{}]: adding column '{}' type {}",
                        tableName, colName, incomingType);
                updateSchema.addColumn(colName, incomingType);

            } else {
                Type currentType = currentFields.get(colName).type();
                if (!currentType.equals(incomingType)) {
                    if (TypeCompatibility.isTypeWidening(currentType, incomingType)) {
                        LOG.info("Schema evolution [{}]: widening column '{}' {} → {}",
                                tableName, colName, currentType, incomingType);
                        updateSchema.updateColumn(colName, incomingType.asPrimitiveType());
                    } else {
                        // Should have been caught in the pre-scan above.
                        throw new IncompatibleTypeChangeException(
                                colName, currentType, incomingType);
                    }
                }
                // Unchanged columns: no action needed.
            }
        }

        // Dropped columns: Iceberg retains them automatically with NULL values
        // for new rows — no explicit action required (design §6.6, Property 7).

        updateSchema.commit();
        LOG.info("Schema evolution committed for table [{}]", tableName);
    }
}
