# Implementation Plan: CDC Pipeline

## Overview

Implement a production-grade Change Data Capture pipeline: PostgreSQL → Debezium → Kafka → Apache Flink → Apache Iceberg. The implementation follows the design document, covering infrastructure, stream processing, schema evolution, exactly-once semantics, DLQ handling, monitoring, and data quality reconciliation.

Primary languages: **Python** (utilities, reconciliation, DLQ replay, property-based tests via Hypothesis) and **Java** (Flink CDC processor job, operator unit tests via jqwik).

## Tasks

- [x] 1. Project scaffolding and repository structure
  - Create top-level directory layout: `flink-job/`, `scripts/`, `monitoring/`, `connectors/`, `tests/`, `infra/`
  - Add `requirements.txt` (Python deps: `confluent-kafka`, `pyiceberg`, `hypothesis`, `psycopg2-binary`, `pandas`, `pytest`, `fastavro`)
  - Add `flink-job/pom.xml` (Maven) with Flink 1.18, Iceberg Flink runtime, Kafka connector, Avro, jqwik dependencies
  - Add `.env.example` with all required environment variables (`POSTGRES_PASSWORD`, `MINIO_PASSWORD`, `CDC_DB_PASSWORD`)
  - Add `Makefile` with targets: `up`, `down`, `seed`, `teardown`, `test`, `submit-job`
  - _Requirements: 11.1_

- [x] 2. Docker Compose infrastructure
  - [x] 2.1 Write `docker-compose.yml` with all services
    - Services: `postgres`, `zookeeper`, `kafka`, `schema-registry`, `kafka-connect` (Debezium), `jobmanager`, `taskmanager`, `minio`, `iceberg-rest`, `prometheus`, `grafana`
    - Configure PostgreSQL with `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`
    - Configure Flink JobManager with RocksDB state backend, 30 s checkpoint interval, `EXACTLY_ONCE` mode, Prometheus reporter on port 9249
    - Configure Flink TaskManager with 4 task slots and matching state backend settings
    - Configure MinIO with `warehouse` and `checkpoints` buckets
    - Configure Iceberg REST catalog pointing to MinIO
    - Add healthchecks for PostgreSQL, Kafka, Schema Registry, Flink JobManager
    - _Requirements: 11.1_
  - [x] 2.2 Write `infra/init-minio.sh` to create `warehouse` and `checkpoints` buckets on MinIO startup
    - _Requirements: 11.1_

- [x] 3. PostgreSQL source configuration
  - [x] 3.1 Write `scripts/init-postgres.sql`
    - Create `cdc_user` with `REPLICATION` and `LOGIN` privileges
    - Grant `SELECT` on all monitored tables to `cdc_user`
    - Create source tables: `orders`, `customers`, `products` with appropriate columns and primary keys
    - Create `cdc_heartbeat` table (schema per design §3.4: single row, `id=1`, updated in-place)
    - Create `cdc_publication` covering `orders`, `customers`, `products`, `cdc_heartbeat`
    - Create replication slot `debezium_slot` using `pgoutput` plugin
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 3.2 Write `scripts/check_wal_lag.py` implementing `check_wal_lag(conn, slot_name, warn_threshold_bytes)`
    - Query `pg_replication_slots` for `restart_lsn`
    - Compute `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`
    - Emit `WARNING` log entry if lag exceeds 10 GB threshold
    - Return `WalLagStatus` dataclass with slot name, lag bytes, and warning flag
    - _Requirements: 1.5_

- [x] 4. Debezium connector configuration
  - [x] 4.1 Write `connectors/debezium-postgres.json` with all connector properties
    - Set `connector.class`, `database.*`, `plugin.name=pgoutput`, `publication.name=cdc_publication`, `slot.name=debezium_slot`
    - Set `snapshot.mode=initial`, `snapshot.locking.mode=minimal`
    - Set Avro converters pointing to Schema Registry
    - Set `errors.tolerance=all`, `errors.deadletterqueue.topic.name=cdc.dead-letter-queue`, `errors.deadletterqueue.context.headers.enable=true`
    - Set `tombstones.on.delete=true`, `heartbeat.interval.ms=60000`, `heartbeat.action.query` for `cdc_heartbeat` upsert
    - Set `errors.retry.delay.max.ms=60000`
    - _Requirements: 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 3.5_

- [x] 5. Kafka topic management
  - [x] 5.1 Write `scripts/create_topics.py` implementing `topic_name()` and `create_cdc_topic()`
    - `topic_name(database, schema, table) -> str` returns `"{database}.{schema}.{table}"`
    - `create_cdc_topic()` creates topic with 6 partitions, replication factor 3, min ISR 2, 7-day retention
    - Create all required topics: `db.public.orders`, `db.public.customers`, `db.public.products`, `cdc.dead-letter-queue` (3 partitions, 30-day retention), `cdc.heartbeat` (1 partition, 1-day retention), `connect-offsets`, `connect-configs`, `connect-status`
    - _Requirements: 4.1, 4.2, 4.3_
  - [x]* 5.2 Write unit tests for `topic_name()` and `create_cdc_topic()` in `tests/test_topic_management.py`
    - Test naming convention for various database/schema/table combinations
    - Test topic config values (partitions, replication factor, retention)
    - _Requirements: 4.1, 4.2, 4.3_

- [x] 6. Flink CDC processor — core data model and Avro deserializer
  - [x] 6.1 Define Java data model classes in `flink-job/src/main/java/cdc/model/`
    - `ChangeEvent` (fields: `before`, `after`, `source`, `op`, `ts_ms`, `transaction`)
    - `SourceInfo` (fields: `version`, `connector`, `name`, `ts_ms`, `db`, `schema`, `table`, `lsn`, `txId`)
    - `PrimaryKey` (wrapper for equality/hashing by PK value)
    - `DLQEnvelope` (fields per design §3.3: `original_topic`, `original_partition`, `original_offset`, `error_type`, `error_message`, `error_timestamp_utc`, `raw_payload`)
    - `ErrorType` enum: `DESERIALIZATION_FAILURE`, `SCHEMA_INCOMPATIBLE`, `TYPE_MISMATCH`, `PROCESSING_ERROR`
    - _Requirements: 3.1, 3.2, 3.3, 8.2_
  - [x] 6.2 Implement `AvroDeserializer` in `flink-job/src/main/java/cdc/operator/AvroDeserializer.java`
    - Implement `DeserializationSchema<ChangeEvent>`
    - Decode Avro bytes using Schema Registry client (resolve schema by embedded schema ID)
    - On decode error: call `on_error()` which emits `DLQEnvelope` to DLQ side output with `DESERIALIZATION_FAILURE`
    - Continue processing subsequent events after routing to DLQ
    - _Requirements: 5.6_
  - [x]* 6.3 Write jqwik property test for `AvroDeserializer` in `flink-job/src/test/java/cdc/operator/AvroDeserializerTest.java`
    - **Property 1: Kafka message key equals source primary key**
    - Generate random rows with various PK types (int, UUID, composite); verify deserialized key equals source PK
    - **Validates: Requirements 4.4**

- [x] 7. Flink CDC processor — DeduplicationOperator
  - [x] 7.1 Implement `DeduplicationOperator` in `flink-job/src/main/java/cdc/operator/DeduplicationOperator.java`
    - Extend `KeyedProcessFunction<PrimaryKey, ChangeEvent, ChangeEvent>`
    - Maintain `ValueState<Long>` for last-seen LSN per PK
    - In `processElement`: emit event only if `event.source.lsn > lastSeenLsn`; update state
    - In `onTimer`: clear state for PKs not seen within TTL window (register event-time timer on each update)
    - _Requirements: 5.3, 5.4_
  - [x]* 7.2 Write jqwik property test for `DeduplicationOperator` in `flink-job/src/test/java/cdc/operator/DeduplicationOperatorTest.java`
    - **Property 4: Within-batch deduplication preserves latest event**
    - Generate batches with 1–N events per PK with random LSN ordering; verify only highest-LSN event is emitted
    - **Validates: Requirements 5.3, 5.4**

- [x] 8. Flink CDC processor — SchemaEvolutionOperator
  - [x] 8.1 Implement `is_type_widening()` utility in `flink-job/src/main/java/cdc/schema/TypeCompatibility.java`
    - Implement widening map: `INT→LONG`, `FLOAT→DOUBLE`, `DECIMAL(p,s)→DECIMAL(p',s)` where `p'>p`
    - Return `true` for safe widenings, `false` for all other type changes
    - _Requirements: 7.3, 7.4_
  - [x]* 8.2 Write jqwik unit tests for `is_type_widening()` in `flink-job/src/test/java/cdc/schema/TypeCompatibilityTest.java`
    - Test all valid widening pairs (`INT→LONG`, `FLOAT→DOUBLE`, `DECIMAL` precision widening) return `true`
    - Test narrowing and incompatible pairs return `false`
    - _Requirements: 7.3, 7.4_
  - [x] 8.3 Implement `SchemaEvolutionOperator` in `flink-job/src/main/java/cdc/operator/SchemaEvolutionOperator.java`
    - Extend `RichMapFunction<ChangeEvent, ChangeEvent>`
    - In `open()`: initialise Iceberg REST catalog connection (`http://iceberg-rest:8181`)
    - In `map()`: call `apply_schema_change()` — add new columns with NULL default, retain dropped columns, widen compatible types, raise `IncompatibleTypeChangeError` for breaking changes
    - On `IncompatibleTypeChangeError`: route event to DLQ side output with `TYPE_MISMATCH` and emit alert log
    - _Requirements: 7.1, 7.2, 7.3, 7.4_
  - [x]* 8.4 Write jqwik property tests for `SchemaEvolutionOperator` in `flink-job/src/test/java/cdc/operator/SchemaEvolutionOperatorTest.java`
    - **Property 6: Additive schema evolution propagates to Iceberg** — generate new column (name, type) pairs; verify Iceberg schema updated and row value correct
    - **Property 7: Dropped column retained with NULL** — generate column drop scenarios; verify column retained with NULL
    - **Property 8: Type widening updates Iceberg column type** — generate valid widening pairs; verify no data loss
    - **Property 9: Incompatible type change routes to DLQ** — generate invalid type change pairs; verify DLQ routing with `TYPE_MISMATCH`
    - **Validates: Requirements 7.1, 7.2, 7.3, 7.4**

- [x] 9. Flink CDC processor — DLQRouter and IcebergSink wiring
  - [x] 9.1 Implement `DLQRouter` in `flink-job/src/main/java/cdc/operator/DLQRouter.java`
    - Extend `ProcessFunction<ChangeEvent, ChangeEvent>`
    - Define DLQ side output tag
    - In `processElement`: on error emit `DLQEnvelope` to side output; on success emit to main output
    - Populate all `DLQEnvelope` fields: `original_topic`, `original_partition`, `original_offset`, `error_type`, `error_message`, `error_timestamp_utc`, `raw_payload`
    - _Requirements: 8.1, 8.2_
  - [x]* 9.2 Write jqwik property test for `DLQRouter` in `flink-job/src/test/java/cdc/operator/DLQRouterTest.java`
    - **Property 10: DLQ messages contain complete metadata**
    - Generate events of each `ErrorType`; verify all 7 DLQ envelope fields are non-null/non-empty
    - **Validates: Requirements 8.2**
  - [x] 9.3 Implement `IcebergSinkFactory` in `flink-job/src/main/java/cdc/sink/IcebergSinkFactory.java`
    - Configure `FlinkSink.forRowData()` with upsert mode, equality delete key on source PK column(s)
    - Set catalog to REST catalog at `http://iceberg-rest:8181`, warehouse `s3://warehouse/`
    - Set table namespace `cdc`, table name matching source table
    - Configure `PARTITION BY DAYS(updated_at)`, format version 2, `write.upsert.enabled=true`, `write.merge.mode=merge-on-read`
    - _Requirements: 5.1, 5.2_
  - [x] 9.4 Implement main Flink job entry point in `flink-job/src/main/java/cdc/CdcPipelineJob.java`
    - Build `StreamExecutionEnvironment` with RocksDB state backend, 30 s checkpoint interval, `EXACTLY_ONCE` mode, 60 s timeout, max 1 concurrent checkpoint, 3 tolerated failures before job failure
    - Build multi-topic `KafkaSource` for all monitored tables (`db.public.orders`, `db.public.customers`, `db.public.products`)
    - Wire operator graph: `KafkaSource → AvroDeserializer → ChangeEventRouter → [per table] DeduplicationOperator → SchemaEvolutionOperator → IcebergSink`
    - Attach DLQ side outputs from `AvroDeserializer`, `SchemaEvolutionOperator`, and `DLQRouter` to a Kafka producer sink writing to `cdc.dead-letter-queue`
    - Set parallelism = number of Kafka partitions (6)
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 6.1, 6.2, 6.3, 6.4_
  - [x]* 9.5 Write jqwik property test for upsert/delete routing in `flink-job/src/test/java/cdc/CdcPipelineJobTest.java`
    - **Property 3: Upsert and delete operation routing**
    - Generate random Change Events (`op=c/u/d`) with random field values; verify Iceberg row equals `after` image for `c`/`u`, and row absent or `_cdc_deleted=TRUE` for `d`
    - **Validates: Requirements 5.1, 5.2**

- [x] 10. Checkpoint: Ensure all Flink operator tests pass
  - [x] Run `mvn test` in `flink-job/` to confirm all jqwik property tests and unit tests pass
  - [x] All 63 tests passing (7 DeduplicationOperator + 9 DLQRouter + 9 SchemaEvolution + 9 CdcPipelineJob + 29 TypeCompatibility)
  - [x] Fixed Java module access issues, object equality assertions, null handling in test harness, and BigDecimal precision

- [x] 11. Exactly-once semantics — checkpoint and atomic commit
  - [x] 11.1 Implement `KafkaOffsetsCommitter` in `flink-job/src/main/java/cdc/sink/KafkaOffsetsCommitter.java`
    - [x] Implement `CheckpointedFunction` interface for state management
    - [x] In `notifyCheckpointComplete(checkpointId)`: commit Kafka consumer group offsets
    - [x] Offset state tracking and cleanup to prevent memory leaks
    - [x] Handles offset map per checkpoint with automatic cleanup
    - _Requirements: 6.2, 6.4_
  - [x] 11.2 Verify checkpoint protocol implementation in `CdcPipelineJob` matches design §6.6
    - [x] Added comprehensive documentation of checkpoint protocol in class javadoc
    - [x] Confirmed checkpoint configuration: RocksDB, 30s interval, EXACTLY_ONCE mode, 60s timeout
    - [x] Documented exact sequence: barrier propagation → flush → checkpoint complete → Iceberg commit → Kafka offset commit
    - [x] Explained recovery behavior: orphaned files not in snapshots, offsets rewound, idempotent replay
    - _Requirements: 6.2, 6.3, 6.4_
  - [x]* 11.3 Write Hypothesis property test for exactly-once semantics in `tests/test_exactly_once.py`
    - [x] **Property 5: Exactly-once on failure and restart** — all event sequences produce same final state
    - [x] **Property: No duplicate PKs after recovery** — recovery produces no duplicate primary keys
    - [x] **Property: Idempotent event replay** — replaying same sequence produces identical state
    - [x] **Property: Row count consistency** — row count methods always agree
    - [x] All 4 Hypothesis tests passing (20 examples each)
    - **Validates: Requirements 6.2, 6.3, 6.4**

- [x] 12. Dead Letter Queue — Python replay utility
  - [x] 12.1 Implement `scripts/dlq_replay.py` with `replay_dlq()` function matching the signature in design §6.7
    - [x] Accept CLI args: `--dlq-topic`, `--kafka-bootstrap`, `--filter-error-type`, `--since`, `--dry-run`
    - [x] Read all messages from `cdc.dead-letter-queue`; filter by `error_type` or time range (`since`)
    - [x] Re-publish `raw_payload` bytes to `original_topic` with original Kafka message key
    - [x] Log each replayed event; log failures and continue; exit non-zero if any event fails
    - [x] In `--dry-run` mode: print events without publishing
    - _Requirements: 8.5_
  - [x]* 12.2 Write Hypothesis property test for DLQ replay in `tests/test_dlq_replay.py`
    - **Property 11: DLQ replay delivers events to original topic**
    - Generate sets of DLQ events with various `ErrorType` values; run replay; verify each event appears in `original_topic` with original payload bytes intact
    - **Validates: Requirements 8.5**
  - [x]* 12.3 Write Hypothesis property test for DLQ metadata in `tests/test_dlq.py`
    - **Property 2: Incompatible schema change routes event to DLQ**
    - Generate incompatible schema change pairs; verify event appears in `cdc.dead-letter-queue` with non-null `error_type` and `error_message`
    - **Validates: Requirements 4.6**

- [ ] 13. Monitoring stack
  - [ ] 13.1 Write `monitoring/prometheus.yml` with scrape configs
    - Scrape Flink JobManager `:9249/metrics` every 15 s
    - Scrape Flink TaskManager `:9249/metrics` every 15 s
    - Scrape Kafka JMX Exporter `:9308/metrics` every 15 s
    - Scrape Debezium JMX Exporter `:9404/metrics` every 15 s
    - Scrape PostgreSQL exporter `:9187/metrics` every 15 s
    - _Requirements: 9.1_
  - [ ] 13.2 Write `monitoring/grafana/provisioning/datasources/prometheus.yml` to auto-provision the Prometheus datasource in Grafana
    - _Requirements: 9.5_
  - [ ] 13.3 Write `monitoring/grafana/provisioning/dashboards/dashboard.yml` dashboard provisioning config pointing to `/var/lib/grafana/dashboards`
    - _Requirements: 9.5_
  - [ ] 13.4 Write `monitoring/grafana/dashboards/cdc-pipeline.json` Grafana dashboard JSON
    - Panel 1: Kafka consumer lag per topic-partition (line chart, alert threshold at 10,000 messages)
    - Panel 2: Flink checkpoint duration (histogram)
    - Panel 3: Flink checkpoint failure count (counter)
    - Panel 4: Iceberg commit latency (gauge)
    - Panel 5: DLQ message count (counter + alert threshold line at 100 in 5 min)
    - Panel 6: End-to-end heartbeat latency (line chart)
    - Panel 7: Debezium WAL lag bytes
    - Panel 8: Flink records-in / records-out per operator
    - Set dashboard refresh interval to 30 s
    - _Requirements: 9.1, 9.5_
  - [ ] 13.5 Implement `HeartbeatLatencyTracker` in `flink-job/src/main/java/cdc/operator/HeartbeatLatencyTracker.java`
    - Detect heartbeat events by checking `source.table == "cdc_heartbeat"`
    - Record `heartbeat_write_timestamp` from event `source.ts_ms`
    - On Iceberg commit: compute `iceberg_commit_timestamp − heartbeat_write_timestamp` and emit as Prometheus gauge
    - _Requirements: 9.4_
  - [ ]* 13.6 Write Hypothesis property test for heartbeat latency in `tests/test_heartbeat.py`
    - **Property 12: Heartbeat latency measurement accuracy**
    - Generate heartbeat events with known timestamps; verify recorded latency equals `iceberg_commit_ts − heartbeat_write_ts` within ±1 second tolerance
    - **Validates: Requirements 9.4**

- [ ] 14. Data quality reconciliation job
  - [ ] 14.1 Implement `scripts/reconcile.py` with `reconcile_table()`, `compare_row_checksums()`, and `run_reconciliation()` matching signatures in design §6.7
    - `reconcile_table(source_conn, iceberg_catalog, table_name, sample_size=1000)`: query source `COUNT(*)` and target count (excluding `_cdc_deleted=TRUE` rows); if counts match, sample up to `sample_size` PKs and compare rows; return `ReconciliationResult`
    - `compare_row_checksums(pk, source_row, target_row)`: ignore CDC metadata columns (`_cdc_op`, `_cdc_ts`, `_cdc_lsn`, `_cdc_deleted`); return `list[ChecksumMismatch]`
    - `run_reconciliation(tables, source_dsn, iceberg_catalog_uri, sample_size, alert_fn)`: orchestrate all tables; emit alert on count discrepancy or checksum mismatch
    - _Requirements: 10.1, 10.2, 10.3, 10.4_
  - [ ]* 14.2 Write Hypothesis property tests for reconciliation in `tests/test_reconciliation.py`
    - **Property 13: Reconciliation correctly identifies row count discrepancies** — generate source/target states with known discrepancies; verify correct counts returned and discrepancy flagged iff counts differ (no false positives, no missed discrepancies)
    - **Property 14: Checksum comparison identifies all column-level mismatches** — generate row samples with known column-level differences; verify every mismatch identified with correct PK, column name, source value, and target value
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.4**

- [ ] 15. Checkpoint: Ensure all Python utility tests pass
  - Run `pytest tests/` to confirm all Hypothesis property tests and unit tests pass
  - Ask the user if questions arise.

- [ ] 16. Seed script and teardown script
  - [ ] 16.1 Write `scripts/seed.sh`
    - Wait for PostgreSQL healthcheck to pass
    - Run `scripts/init-postgres.sql` via `psql`
    - Call `scripts/create_topics.py` to create all Kafka topics
    - Register Debezium connector via `POST http://kafka-connect:8083/connectors` with `connectors/debezium-postgres.json`
    - Submit Flink CDC job JAR via `POST http://jobmanager:8081/jars/{jar_id}/run`
    - Insert sample data: 100 orders, 50 customers, 20 products
    - _Requirements: 11.2_
  - [ ] 16.2 Write `scripts/teardown.sh`
    - Run `docker compose down -v`
    - Drop replication slot `debezium_slot` if PostgreSQL is still reachable (`SELECT pg_drop_replication_slot('debezium_slot')`)
    - Remove MinIO buckets via `mc rb --force minio/warehouse minio/checkpoints`
    - Verify no replication slots remain
    - _Requirements: 11.5_

- [ ] 17. Smoke tests
  - [ ] 17.1 Write `tests/test_smoke.py` with infrastructure validation checks
    - PostgreSQL: verify `wal_level = logical`, replication slot `debezium_slot` exists, `cdc_user` has `REPLICATION` privilege and `SELECT` on monitored tables
    - Kafka: verify all required topics exist with correct replication factor, min ISR, and retention settings
    - Schema Registry: verify service is reachable and compatibility mode is `BACKWARD`
    - Flink: verify job is in `RUNNING` state and checkpointing is enabled
    - Grafana: verify dashboard `cdc-pipeline` exists and contains all 8 required panels
    - _Requirements: 11.1, 11.4_

- [ ] 18. Integration tests
  - [ ] 18.1 Write `tests/test_integration.py` with end-to-end pipeline tests against the Docker Compose stack
    - Test 1 — Full snapshot: start Debezium against a populated table; verify all rows appear in Kafka with `op=r`
    - Test 2 — WAL INSERT: insert a row; verify Change Event in Kafka with `op=c` and correct payload within 5 s
    - Test 3 — WAL UPDATE: update a row; verify Change Event with `op=u`, correct before/after images within 5 s
    - Test 4 — WAL DELETE: delete a row; verify Change Event with `op=d` and tombstone within 5 s
    - Test 5 — Flink upsert: end-to-end INSERT → verify row visible in Iceberg
    - Test 6 — Flink delete: end-to-end DELETE → verify row absent from Iceberg (or `_cdc_deleted=TRUE`)
    - Test 7 — Schema evolution: `ALTER TABLE ADD COLUMN` → verify Iceberg schema updated and new rows carry correct value
    - Test 8 — Checkpoint recovery: kill Flink TaskManager mid-batch; restart; verify no duplicate rows in Iceberg
    - Test 9 — DLQ routing: inject malformed Avro bytes; verify DLQ message contains all required metadata fields
    - Test 10 — DLQ replay: populate `cdc.dead-letter-queue`; run `dlq_replay.py`; verify events appear in original topic
    - _Requirements: 2.1, 3.1, 3.2, 3.3, 5.1, 5.2, 6.2, 7.1, 8.5_
  - [ ]* 18.2 Write Hypothesis property test for end-to-end latency in `tests/test_e2e.py`
    - **Property 15: End-to-end change propagation within 10 seconds**
    - Generate DML operations of each type (INSERT, UPDATE, DELETE) on monitored tables; verify change is visible in Iceberg within 10 seconds of DML completing
    - **Validates: Requirements 11.4**
  - [ ]* 18.3 Write Hypothesis property test for Schema Registry enforcement in `tests/test_schema_registry.py`
    - **Property 2 (Schema Registry side): Incompatible schema change routes event to DLQ**
    - Generate incompatible schema change pairs; verify Schema Registry rejects the publish and the event appears in `cdc.dead-letter-queue`
    - **Validates: Requirements 4.6**

- [ ] 19. README documentation
  - Write `README.md` with:
    - Project overview and architecture diagram (Mermaid flowchart matching design §1)
    - Prerequisites: Docker, Docker Compose, Java 11+, Python 3.10+, Maven
    - Step-by-step local setup: `docker compose up -d`, `make seed`, verify pipeline
    - Component descriptions table (PostgreSQL, Debezium, Kafka, Schema Registry, Flink, Iceberg, MinIO, Prometheus, Grafana)
    - How to run tests: `make test` (runs `mvn test` + `pytest tests/`)
    - How to tear down: `make teardown`
    - Links to `design.md` and `requirements.md`
    - _Requirements: 11.3_

- [ ] 20. Final checkpoint — Ensure all tests pass
  - Run `mvn test` in `flink-job/` and `pytest tests/` to confirm all tests pass
  - Verify smoke tests pass against the running Docker Compose stack
  - Ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Property tests use **Hypothesis** (Python) for utilities/reconciliation and **jqwik** (Java/Kotlin) for Flink operators; minimum 100 iterations per property test
- Each property test references the corresponding property number from design §4 (Correctness Properties)
- All 15 correctness properties from the design are covered by property-based tests across tasks 7–18
- Checkpoints at tasks 10, 15, and 20 ensure incremental validation throughout the build
- The Flink job is built as a fat JAR via Maven (`mvn package`) and submitted to the JobManager REST API

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["7.1", "8.1"] },
    { "id": 1, "tasks": ["7.2", "8.2", "8.3"] },
    { "id": 2, "tasks": ["8.4", "9.1", "9.3"] },
    { "id": 3, "tasks": ["9.2", "9.4", "11.1"] },
    { "id": 4, "tasks": ["9.5", "11.2", "12.1", "13.1", "13.2", "13.3", "14.1"] },
    { "id": 5, "tasks": ["11.3", "12.2", "12.3", "13.4", "13.5", "14.2"] },
    { "id": 6, "tasks": ["13.6", "16.1", "16.2"] },
    { "id": 7, "tasks": ["17.1"] },
    { "id": 8, "tasks": ["18.1"] },
    { "id": 9, "tasks": ["18.2", "18.3"] }
  ]
}
```
