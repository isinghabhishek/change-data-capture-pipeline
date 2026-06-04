package cdc.model;

import java.io.Serializable;

/**
 * Represents the Debezium source metadata block within a Change Event.
 * Maps to the {@code source} field in the Debezium Avro envelope.
 */
public class SourceInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Debezium connector version string. */
    public String version;

    /** Connector type (e.g. "postgresql"). */
    public String connector;

    /** Logical server name as configured in Debezium. */
    public String name;

    /** Millisecond epoch timestamp from the database server at event time. */
    public long ts_ms;

    /** Source database name. */
    public String db;

    /** Source schema name. */
    public String schema;

    /** Source table name. */
    public String table;

    /** PostgreSQL WAL Log Sequence Number (nullable). */
    public Long lsn;

    /** PostgreSQL transaction ID (nullable). */
    public Long txId;

    /** No-arg constructor required by Flink POJO serialisation. */
    public SourceInfo() {}

    /** All-args constructor for convenience. */
    public SourceInfo(String version, String connector, String name, long ts_ms,
                      String db, String schema, String table, Long lsn, Long txId) {
        this.version   = version;
        this.connector = connector;
        this.name      = name;
        this.ts_ms     = ts_ms;
        this.db        = db;
        this.schema    = schema;
        this.table     = table;
        this.lsn       = lsn;
        this.txId      = txId;
    }

    @Override
    public String toString() {
        return "SourceInfo{" +
               "version='" + version + '\'' +
               ", connector='" + connector + '\'' +
               ", name='" + name + '\'' +
               ", ts_ms=" + ts_ms +
               ", db='" + db + '\'' +
               ", schema='" + schema + '\'' +
               ", table='" + table + '\'' +
               ", lsn=" + lsn +
               ", txId=" + txId +
               '}';
    }
}
