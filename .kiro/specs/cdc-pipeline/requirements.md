# Requirements Document

## Introduction

This document defines the requirements for a Change Data Capture (CDC) pipeline — Project 5 in a data engineering portfolio series. The pipeline captures row-level changes (inserts, updates, deletes) from a PostgreSQL source database in real-time and propagates them to an Apache Iceberg lakehouse via Debezium, Kafka, and Apache Flink. The system demonstrates CDC patterns, log-based replication, upsert-at-scale semantics, real-time data synchronization, and exactly-once delivery guarantees.

## Glossary

- **CDC_Pipeline**: The end-to-end system described in this document.
- **Source_DB**: The PostgreSQL database whose changes are captured.
- **Debezium**: The open-source CDC connector that reads the PostgreSQL WAL and publishes change events to Kafka.
- **Kafka**: The distributed event streaming platform that acts as the change-event transport layer.
- **Flink**: The Apache Flink stream-processing engine that consumes Kafka events and writes to Iceberg.
- **Iceberg**: The Apache Iceberg table format used as the target lakehouse storage layer.
- **Change_Event**: A Debezium-produced record representing a single INSERT, UPDATE, or DELETE operation on a source table row.
- **WAL**: PostgreSQL Write-Ahead Log — the source of truth for log-based replication.
- **Upsert**: A write operation that inserts a new row or updates an existing row based on a primary key.
- **Exactly_Once**: A delivery semantic guaranteeing each Change_Event is reflected in the target exactly one time, with no duplicates and no omissions.
- **Schema_Registry**: A service (e.g., Confluent Schema Registry) that stores and validates Avro/JSON schemas for Kafka topics.
- **Dead_Letter_Queue**: A Kafka topic that receives Change_Events that could not be processed successfully after exhausting retries.
- **Checkpoint**: A Flink mechanism that periodically snapshots operator state to enable fault-tolerant recovery.
- **Snapshot**: An initial full-table read performed by Debezium before streaming incremental WAL changes.

---

## Requirements

### Requirement 1: Source Database Configuration

**User Story:** As a data engineer, I want PostgreSQL configured for logical replication, so that Debezium can read the WAL and capture all row-level changes.

#### Acceptance Criteria

1. THE Source_DB SHALL have `wal_level` set to `logical` to enable WAL-based CDC.
2. THE Source_DB SHALL have a dedicated replication slot created for Debezium to consume WAL events without data loss.
3. THE Source_DB SHALL have a dedicated replication user with `REPLICATION` and `LOGIN` privileges and SELECT access on all monitored tables.
4. WHEN the replication slot is active, THE Source_DB SHALL retain WAL segments until Debezium acknowledges consumption.
5. IF the replication slot falls more than 10 GB behind, THEN THE Source_DB SHALL emit a warning log entry to prevent unbounded WAL accumulation.

---

### Requirement 2: Initial Snapshot

**User Story:** As a data engineer, I want Debezium to perform an initial consistent snapshot of existing source tables, so that the Iceberg target starts from a complete baseline before streaming incremental changes.

#### Acceptance Criteria

1. WHEN Debezium starts for the first time against a source table, THE Debezium SHALL perform a consistent snapshot read of all existing rows before switching to WAL streaming.
2. WHEN the snapshot is in progress, THE Debezium SHALL hold a table-level read lock for the minimum duration required to establish a consistent LSN (Log Sequence Number).
3. WHEN the snapshot completes, THE Debezium SHALL record the LSN offset so that subsequent restarts resume from WAL streaming rather than re-snapshotting.
4. IF the snapshot is interrupted before completion, THEN THE Debezium SHALL restart the snapshot from the beginning on the next startup.

---

### Requirement 3: Change Event Capture

**User Story:** As a data engineer, I want Debezium to capture every INSERT, UPDATE, and DELETE on monitored tables, so that no source change is lost in transit.

#### Acceptance Criteria

1. WHEN a row is inserted into a monitored table, THE Debezium SHALL publish a Change_Event with operation type `c` (create) to the corresponding Kafka topic within 5 seconds.
2. WHEN a row is updated in a monitored table, THE Debezium SHALL publish a Change_Event with operation type `u` (update) containing both the before-image and after-image of the row within 5 seconds.
3. WHEN a row is deleted from a monitored table, THE Debezium SHALL publish a Change_Event with operation type `d` (delete) containing the before-image of the row within 5 seconds.
4. THE Debezium SHALL preserve the WAL commit order of Change_Events within a single Kafka partition.
5. IF Debezium loses connectivity to the Source_DB, THEN THE Debezium SHALL retry the connection with exponential backoff up to a maximum interval of 60 seconds.

---

### Requirement 4: Kafka Topic Management

**User Story:** As a data engineer, I want Kafka topics structured and configured correctly, so that Change_Events are durable, ordered per table, and consumable by downstream processors.

#### Acceptance Criteria

1. THE CDC_Pipeline SHALL create one Kafka topic per monitored source table, named using the pattern `<database>.<schema>.<table>`.
2. THE CDC_Pipeline SHALL configure each Kafka topic with a replication factor of at least 3 and a minimum in-sync replicas setting of 2.
3. THE CDC_Pipeline SHALL configure topic retention to at least 7 days to allow consumer lag recovery.
4. WHEN a Change_Event is published, THE Debezium SHALL use the primary key of the changed row as the Kafka message key to ensure partition-level ordering per row.
5. THE Schema_Registry SHALL store the Avro schema for each Kafka topic and enforce schema compatibility on every new schema version.
6. IF a producer attempts to publish a Change_Event with an incompatible schema, THEN THE Schema_Registry SHALL reject the publish and THE Debezium SHALL route the event to the Dead_Letter_Queue.

---

### Requirement 5: Stream Processing with Flink

**User Story:** As a data engineer, I want Flink to consume Change_Events from Kafka and apply upsert logic to Iceberg, so that the target table reflects the current state of the source table in near real-time.

#### Acceptance Criteria

1. WHEN Flink reads a Change_Event with operation type `c` or `u`, THE Flink SHALL perform an Upsert into the target Iceberg table keyed on the source primary key.
2. WHEN Flink reads a Change_Event with operation type `d`, THE Flink SHALL mark the corresponding row as deleted in the target Iceberg table.
3. THE Flink SHALL process Change_Events in WAL commit order within each Kafka partition.
4. WHEN multiple Change_Events for the same primary key arrive within a single Flink micro-batch, THE Flink SHALL apply only the latest Change_Event for that key to the Iceberg table.
5. THE Flink SHALL emit Iceberg commits at a configurable interval, with a default of 60 seconds, to balance latency and small-file overhead.
6. IF a Change_Event cannot be deserialized, THEN THE Flink SHALL route the raw Kafka message to the Dead_Letter_Queue and continue processing subsequent events.

---

### Requirement 6: Exactly-Once Semantics

**User Story:** As a data engineer, I want the pipeline to guarantee exactly-once delivery end-to-end, so that the Iceberg target never contains duplicate or missing rows due to failures or retries.

#### Acceptance Criteria

1. THE Flink SHALL enable checkpointing at a configurable interval, with a default of 30 seconds, using a durable state backend (e.g., RocksDB with S3 remote storage).
2. WHEN a Flink job fails and restarts from a Checkpoint, THE Flink SHALL resume Kafka consumption from the committed offset stored in the Checkpoint, producing no duplicate writes to Iceberg.
3. THE Flink SHALL use Iceberg's transactional commit protocol to write Change_Events atomically, ensuring partial writes are never visible to readers.
4. WHEN a Flink Checkpoint succeeds, THE Flink SHALL commit the corresponding Kafka consumer group offsets atomically with the Iceberg table commit.
5. IF a Flink Checkpoint fails three consecutive times, THEN THE Flink SHALL fail the job and trigger an alert so that the pipeline can be restarted from the last successful Checkpoint.

---

### Requirement 7: Schema Evolution

**User Story:** As a data engineer, I want the pipeline to handle source schema changes without manual intervention, so that adding or renaming columns in PostgreSQL does not break downstream Iceberg tables.

#### Acceptance Criteria

1. WHEN a new column is added to a monitored source table, THE Debezium SHALL include the new column in subsequent Change_Events and THE Flink SHALL add the corresponding column to the target Iceberg table before writing the event.
2. WHEN a column is dropped from a monitored source table, THE Flink SHALL retain the column in the target Iceberg table and populate it with `NULL` for rows affected after the drop.
3. WHEN a column data type is widened (e.g., `INT` to `BIGINT`), THE Flink SHALL update the target Iceberg column type to the wider type before writing the event.
4. IF a column data type change is not backward-compatible (e.g., `VARCHAR` to `INT`), THEN THE Flink SHALL route affected Change_Events to the Dead_Letter_Queue and emit an alert describing the incompatible type change.

---

### Requirement 8: Dead Letter Queue Handling

**User Story:** As a data engineer, I want unprocessable events isolated in a Dead Letter Queue, so that pipeline failures are observable and recoverable without blocking the main data flow.

#### Acceptance Criteria

1. THE CDC_Pipeline SHALL maintain a dedicated Dead_Letter_Queue Kafka topic named `cdc.dead-letter-queue`.
2. WHEN an event is routed to the Dead_Letter_Queue, THE CDC_Pipeline SHALL attach metadata including the original topic, partition, offset, error type, error message, and a UTC timestamp.
3. THE CDC_Pipeline SHALL expose a count of Dead_Letter_Queue messages as a monitored metric.
4. WHEN the Dead_Letter_Queue message count exceeds 100 within a 5-minute window, THE CDC_Pipeline SHALL emit an alert to the configured notification channel.
5. THE CDC_Pipeline SHALL provide a replay utility that reads events from the Dead_Letter_Queue and re-submits them to the original Kafka topic after the root cause is resolved.

---

### Requirement 9: Monitoring and Observability

**User Story:** As a data engineer, I want end-to-end pipeline metrics and alerts, so that I can detect lag, failures, and data quality issues before they impact consumers.

#### Acceptance Criteria

1. THE CDC_Pipeline SHALL expose the following metrics: Kafka consumer lag per topic-partition, Flink checkpoint duration, Flink checkpoint failure count, Iceberg commit latency, and Dead_Letter_Queue message count.
2. WHEN Kafka consumer lag for any topic-partition exceeds 10,000 messages, THE CDC_Pipeline SHALL emit a lag alert to the configured notification channel.
3. WHEN end-to-end latency from WAL commit to Iceberg visibility exceeds 60 seconds for three consecutive measurements, THE CDC_Pipeline SHALL emit a latency alert.
4. THE CDC_Pipeline SHALL emit a heartbeat Change_Event every 60 seconds by writing a known row to a dedicated heartbeat table in the Source_DB, and THE Flink SHALL track the time between heartbeat emission and Iceberg commit as the pipeline latency measurement.
5. THE CDC_Pipeline SHALL provide a Grafana dashboard displaying all metrics defined in criterion 1 with a data refresh interval of 30 seconds.

---

### Requirement 10: Data Quality Validation

**User Story:** As a data engineer, I want row counts and checksums validated between source and target, so that I can confirm the Iceberg table is a faithful replica of the source.

#### Acceptance Criteria

1. THE CDC_Pipeline SHALL provide a reconciliation job that compares row counts between each monitored source table and its corresponding Iceberg table on a configurable schedule, with a default of every 6 hours.
2. WHEN the reconciliation job detects a row count discrepancy greater than 0, THE CDC_Pipeline SHALL emit an alert and log the affected table name, expected count, and actual count.
3. THE CDC_Pipeline SHALL provide a column-level checksum comparison for a configurable sample of rows (default: 1,000 rows per table per run) to detect silent data corruption.
4. WHEN a checksum mismatch is detected, THE CDC_Pipeline SHALL log the primary key, column name, source value, and target value for each mismatched row.

---

### Requirement 11: Infrastructure and Deployment

**User Story:** As a data engineer, I want the entire pipeline deployable via Docker Compose for local development and documented for cloud deployment, so that the project is reproducible and portfolio-ready.

#### Acceptance Criteria

1. THE CDC_Pipeline SHALL provide a `docker-compose.yml` that starts all components (PostgreSQL, Debezium, Kafka, Schema Registry, Flink, Iceberg REST catalog, and Grafana) with a single `docker compose up` command.
2. THE CDC_Pipeline SHALL provide a seed script that creates source tables, inserts sample data, and registers the Debezium connector via the Kafka Connect REST API.
3. THE CDC_Pipeline SHALL provide a `README.md` with step-by-step local setup instructions, architecture diagram, and a description of each component's role.
4. WHEN all services are healthy, THE CDC_Pipeline SHALL demonstrate end-to-end change propagation within 10 seconds of a source INSERT, UPDATE, or DELETE.
5. THE CDC_Pipeline SHALL include a `teardown` script that removes all Docker volumes and replication slots to enable a clean restart.
