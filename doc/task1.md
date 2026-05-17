# Task 1: Project Scaffolding and Repository Structure

**Status:** Completed  
**Requirement:** 11.1

---

## Findings

All scaffolding artifacts were created successfully. No blockers encountered.

---

## Completed Items

### Directory Layout

| Directory | Purpose | Placeholder |
|---|---|---|
| `flink-job/` | Maven project for the Flink CDC processor Java job | `.gitkeep` |
| `scripts/` | Shell and Python utility scripts (seed, teardown, WAL lag check, etc.) | `.gitkeep` |
| `monitoring/` | Prometheus and Grafana configuration files | `.gitkeep` |
| `connectors/` | Debezium connector JSON configuration | `.gitkeep` |
| `tests/` | Python test suite (pytest + Hypothesis) | `__init__.py` |
| `infra/` | Infrastructure init scripts (MinIO bucket setup, etc.) | `.gitkeep` |

---

### `requirements.txt`

Python dependencies pinned with minimum versions:

| Package | Version | Purpose |
|---|---|---|
| `confluent-kafka` | >=2.3.0 | Kafka producer/consumer for DLQ replay and topic management |
| `pyiceberg` | >=0.6.0 | Iceberg catalog access for reconciliation job |
| `hypothesis` | >=6.100.0 | Property-based testing framework |
| `psycopg2-binary` | >=2.9.9 | PostgreSQL connectivity for WAL lag check and reconciliation |
| `pandas` | >=2.2.0 | Row-level data comparison in reconciliation |
| `pytest` | >=8.0.0 | Test runner |
| `fastavro` | >=1.9.0 | Avro serialisation/deserialisation in Python utilities |

---

### `flink-job/pom.xml`

Maven build file for the Flink CDC processor fat JAR.

| Dependency | Version | Scope |
|---|---|---|
| `flink-streaming-java` | 1.18.1 | provided |
| `flink-clients` | 1.18.1 | provided |
| `flink-connector-kafka` | 3.1.0-1.18 | compile |
| `iceberg-flink-runtime-1.18` | 1.5.2 | compile |
| `avro` | 1.11.3 | compile |
| `flink-avro` | 1.18.1 | compile |
| `kafka-avro-serializer` (Confluent) | 7.6.0 | compile |
| `kafka-schema-registry-client` (Confluent) | 7.6.0 | compile |
| `flink-statebackend-rocksdb` | 1.18.1 | compile |
| `jqwik` | 1.8.4 | test |
| `junit-jupiter-api/engine/params` | 5.10.2 | test |
| `flink-test-utils` | 1.18.1 | test |

Maven plugins configured:
- `maven-compiler-plugin` 3.12.1 — Java 11 source/target
- `maven-surefire-plugin` 3.2.5 — runs `*Test.java`, `*Tests.java`, `*Properties.java`
- `maven-shade-plugin` 3.5.2 — builds fat JAR with `cdc.CdcPipelineJob` as main class; excludes Flink provided deps and strips signature files

Confluent Maven repository added: `https://packages.confluent.io/maven/`

---

### `.env.example`

Environment variables documented for local development:

| Variable | Description |
|---|---|
| `POSTGRES_USER` | PostgreSQL superuser |
| `POSTGRES_PASSWORD` | PostgreSQL superuser password |
| `POSTGRES_DB` | Source database name |
| `CDC_DB_USER` | Replication user (`cdc_user`) |
| `CDC_DB_PASSWORD` | Replication user password |
| `MINIO_ROOT_USER` | MinIO admin user |
| `MINIO_PASSWORD` | MinIO admin password |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address |
| `SCHEMA_REGISTRY_URL` | Confluent Schema Registry URL |
| `FLINK_JOBMANAGER_HOST` | Flink JobManager hostname |
| `FLINK_JOBMANAGER_PORT` | Flink JobManager REST API port |
| `ICEBERG_REST_URL` | Iceberg REST catalog URL |
| `ICEBERG_WAREHOUSE` | Iceberg warehouse S3 path |
| `GRAFANA_ADMIN_USER` | Grafana admin username |
| `GRAFANA_ADMIN_PASSWORD` | Grafana admin password |

---

### `Makefile`

| Target | Command | Description |
|---|---|---|
| `up` | `docker compose up -d` | Start all services in detached mode |
| `down` | `docker compose down` | Stop all services |
| `seed` | `bash scripts/seed.sh` | Create tables, topics, register Debezium connector, insert sample data |
| `teardown` | `bash scripts/teardown.sh` | Remove Docker volumes and replication slots for clean restart |
| `test` | `cd flink-job && mvn test` + `pytest tests/` | Run Java (jqwik/JUnit 5) and Python (pytest/Hypothesis) test suites |
| `submit-job` | `mvn package` + Flink REST API call | Build fat JAR and submit `cdc.CdcPipelineJob` to JobManager |

---

## Notes

- All directories contain a `.gitkeep` (or `__init__.py` for `tests/`) so they are tracked by git before files are added.
- `flink-job/` also contains `.gitkeep` alongside `pom.xml`; the Java source tree (`src/main/java/`, `src/test/java/`) will be populated in tasks 6–11.
- The `submit-job` Makefile target reads `FLINK_JOBMANAGER_HOST` and `FLINK_JOBMANAGER_PORT` from the environment (set via `.env` or shell export).
