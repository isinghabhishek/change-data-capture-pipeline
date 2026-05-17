.PHONY: up down seed teardown test submit-job

# Start all services in detached mode
up:
	docker compose up -d

# Stop all services
down:
	docker compose down

# Seed source tables, topics, and register Debezium connector
seed:
	bash scripts/seed.sh

# Tear down all volumes and replication slots
teardown:
	bash scripts/teardown.sh

# Run all tests: Java (jqwik/JUnit 5) and Python (pytest + Hypothesis)
test:
	cd flink-job && mvn test
	pytest tests/

# Build fat JAR and submit to Flink JobManager REST API
submit-job:
	cd flink-job && mvn package -DskipTests
	$(eval JAR_ID=$(shell curl -s http://$(FLINK_JOBMANAGER_HOST):$(FLINK_JOBMANAGER_PORT)/jars | python3 -c "import sys,json; jars=json.load(sys.stdin)['files']; print(jars[-1]['id']) if jars else sys.exit(1)"))
	curl -X POST http://$(FLINK_JOBMANAGER_HOST):$(FLINK_JOBMANAGER_PORT)/jars/$(JAR_ID)/run \
		-H "Content-Type: application/json" \
		-d '{"entryClass": "cdc.CdcPipelineJob"}'
