Diskless Storage Docker Setup
==============================

This directory contains Docker Compose configuration for running Kafka with **Diskless/Ursa Storage**. For enabled topics, Ursa is the primary storage layer rather than a tier that sits behind Kafka's local log.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka-1   │     │   Kafka-2   │     │   Kafka-3   │
│  (broker +  │     │  (broker +  │     │  (broker +  │
│ controller) │     │ controller) │     │ controller) │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
       ┌──────▼──────┐          ┌───────▼───────┐
       │    Oxia     │          │     MinIO     │
       │  (metadata) │          │  (S3 storage) │
       └─────────────┘          └───────────────┘
```

- **Oxia**: Distributed metadata store for producer state persistence
- **MinIO**: S3-compatible object storage for Ursa WAL and compacted objects
- **Kafka Brokers**: 3-node cluster with diskless storage enabled

## Prerequisites

- Docker >= 20.10.4
- Docker Compose v2+
- Python >= 3.7 (for building image)
- Java >= 17 (for building Kafka)

## Quick Start

### Full lakehouse E2E (MinIO + Polaris + DuckDB)

`docker-compose-lakehouse.yml` is the complete local lakehouse stack. It uses:

- three Kafka KRaft brokers with diskless Ursa storage;
- Oxia for the Lakestream catalog, WAL indexes, and compaction tasks;
- MinIO for both the Ursa WAL/managed compacted objects and the Iceberg warehouse;
- Apache Polaris as an Iceberg REST catalog;
- the standalone Ursa compactor to materialize Kafka records into an external Iceberg table;
- DuckDB as an on-demand SQL engine and automated E2E verifier.

The standalone Ursa compactor owns compaction-task publication for diskless
topics. It discovers the Lakestream partition logs, publishes their ranges,
decodes the Kafka `MemoryRecords` stored in the Ursa WAL, and writes the managed
and Iceberg outputs. Classic local-log ingestion is not part of this diskless
demo and will use a separate StreamCatalog-based integration.

```text
Kafka producer -> Kafka broker -> Ursa WAL (MinIO)
                                     |
                                     v
                     Ursa compactor (publishes tasks)
                              |           |
                              v           v
                    managed objects   Iceberg table
                                      |     |
                                      v     v
                                  Polaris  MinIO
                                      |
                                      v
                                   DuckDB
```

Build the two local project images first. `URSA_STORAGE_DIR` must point to the
matching local `ursa-storage` checkout:

```bash
URSA_STORAGE_DIR=/path/to/ursa-storage ./build-lakehouse-images.sh
```

Then run the end-to-end assertion:

```bash
./run-lakehouse-demo.sh
```

The verifier creates a diskless topic, writes 100 raw Kafka records, reads all
100 back through Kafka, waits for Ursa to commit the external Iceberg table,
asserts that DuckDB reads the same row count through Polaris, and checks that
the range was materialized exactly once without compactor errors. On success
it removes the demo containers and volumes. To retain the stack for inspection:

```bash
KEEP_RUNNING=true ./run-lakehouse-demo.sh
```

The verifier requires a fresh Compose project so a prior topic or Iceberg
snapshot cannot make an assertion pass accidentally. After a retained or failed
run, use `make destroy-lakehouse` before retrying.

For a long-running local cluster, use:

```bash
make up-lakehouse
make duckdb-lakehouse
```

Inside DuckDB, discover the incarnation-qualified table name and query it:

```sql
SHOW ALL TABLES;
SELECT count(*) FROM lakehouse.default."ursa-lakehouse-e2e-topic-id-<uuid>";
```

Destroy the retained cluster with `make destroy-lakehouse`.

The Polaris service intentionally uses its in-memory development metastore and
static MinIO credentials. It is suitable for a reproducible local E2E, not for
durable or production catalog deployment. DuckDB is on demand rather than a
resident server; use Trino instead when a shared JDBC/HTTP query endpoint is a
requirement. All published ports bind to `127.0.0.1` because the demo uses
fixed development credentials.

### 1. Build the Docker Image

```bash
cd docker/examples/docker-compose-files/cluster/ursa
./build-image.sh
```

This will:
1. Build Kafka with diskless storage support
2. Create Docker image `kafka-diskless:latest`

Build a linux/amd64 (x86_64) image (useful on Apple Silicon):

```bash
./build-image.sh --amd64
```

### Compaction (LocalStack)

For raw Kafka batch to Parquet compaction via an external compactor container, use:

- Cluster compose: `docker-compose-localstack-compaction.yml`
- Demo overlay (create topic + produce/consume raw records + wait for Parquet): `docker-compose-localstack-compaction.demo.yml`

Build the standalone Maven package in the `ursa-storage` repository. The package must contain
`ursa-storage-compact/target/ursa-storage-compact-*.jar` and its runtime dependencies under
`ursa-storage-compact/target/lib/`. Then build the neutral compactor image from this repository:

```bash
# From the ursa-storage repository:
mvn -B -ntp -pl ursa-storage-compact -am -DskipTests clean package

# From the Kafka repository:
docker build -t ursa-compact:standalone-s3-e2e \
  -f docker/examples/docker-compose-files/cluster/ursa/ursa-compactor.Dockerfile \
  </path/to/ursa-storage>
```

Run the full end-to-end demo:

```bash
docker compose -f docker-compose-localstack-compaction.yml up -d
docker compose -f docker-compose-localstack-compaction.yml \
  -f docker-compose-localstack-compaction.demo.yml \
  run --rm raw-consumer
docker compose -f docker-compose-localstack-compaction.yml down -v --remove-orphans
```

Or run the helper script:

```bash
bash ./run-localstack-compaction-demo.sh
```

The raw-byte demo intentionally does not start a schema registry. The compactor runs from
`/opt/ursa` with classpath `/opt/ursa/ursa-storage-compact.jar:/opt/ursa/lib/*`.

### 2. Run the Demo (Recommended)

The easiest way to get started is to run the full demo, which starts the cluster, creates a topic, and runs producer/consumer performance tests:

```bash
make demo
```

This will:
1. Start Oxia, MinIO, and 3 Kafka brokers
2. Create a diskless topic `test-diskless` with 64 partitions
3. Run 2 producer instances (10k msg/s each)
4. Run 1 consumer instance
5. Clean up when done (including on Ctrl+C)

### 3. Manual Setup

If you prefer to start the cluster manually:

```bash
# Start cluster only
make up

# Create a diskless topic
make create-topic

# Or create a custom topic
make create-topic TOPIC=my-topic PARTITIONS=6
```

### 4. Verify Services

```bash
# Check all services are running
make ps

# View Kafka logs
make logs

# List topics
make list-topics
```

### 5. Produce and Consume

**Interactive console producer/consumer:**

```bash
# Start console producer (type messages and press Enter)
make console-producer

# Start console consumer (in another terminal)
make console-consumer
```

**Performance testing:**

```bash
# Run producer perf test (100k messages)
make produce

# Run consumer perf test
make consume

# Test with a custom topic
make produce TOPIC=my-topic
make consume TOPIC=my-topic
```

### 6. Advanced Performance Testing

For more control over performance tests:

```bash
./bin/kafka-producer-perf-test.sh \
  --topic test-diskless \
  --num-records 20000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:29092 \
    acks=1 \
    linger.ms=25 \
    batch.size=102400 \
    buffer.memory=128000000 \
    max.request.size=64000000 \
    max.in.flight.requests.per.connection=100000 \
    enable.idempotence=false
```

## Available Make Commands

| Command | Description |
|---------|-------------|
| `make demo` | Start cluster with full demo (topic + producer/consumer) |
| `make up` | Start cluster only |
| `make down` | Stop all services |
| `make destroy` | Stop all services and remove volumes |
| `make logs` | Follow kafka-1 logs |
| `make ps` | Show running services |
| `make create-topic` | Create a diskless topic |
| `make list-topics` | List all topics |
| `make produce` | Run producer perf test |
| `make consume` | Run consumer perf test |
| `make console-producer` | Start interactive console producer |
| `make console-consumer` | Start interactive console consumer |
| `make build-lakehouse` | Build Kafka and Ursa compactor images for the lakehouse stack |
| `make up-lakehouse` | Start Kafka, Ursa, MinIO, and Polaris |
| `make demo-lakehouse` | Run the Kafka-to-DuckDB E2E and clean up on success |
| `make duckdb-lakehouse` | Open DuckDB attached to the local Polaris catalog |
| `make destroy-lakehouse` | Stop the lakehouse stack and remove its volumes |

**Note**: Diskless topics enforce RF=1 (replication factor of 1) because data is stored in remote storage, not replicated across brokers.

## Ports

| Service | Port | Description |
|---------|------|-------------|
| kafka-1 | 29092 | Kafka broker 1 |
| kafka-2 | 39092 | Kafka broker 2 |
| kafka-3 | 49092 | Kafka broker 3 |
| oxia | 6648 | Oxia metadata service |
| minio | 19000 | MinIO S3 API |
| minio | 19001 | MinIO Console |
| polaris | 18181 | Iceberg REST and management APIs |
| polaris | 18182 | Health and metrics APIs |

## MinIO Console

Access the MinIO console at http://localhost:19001

- **Username**: minioadmin
- **Password**: minioadmin

You can browse the `kafka-ursa` bucket to see Ursa WAL and compacted objects.

## Configuration

### Oxia Settings

Oxia is configured with 8 shards (`--shards=8`) to prevent WAL offset conflicts under high concurrent write load. This is a workaround for [oxia#796](https://github.com/oxia-db/oxia/issues/796).

### Environment Variables

Diskless storage is configured via environment variables in `docker-compose.yml`:

| Variable | Description |
|----------|-------------|
| `KAFKA_URSA_STORAGE_ENABLE` | Enable diskless storage (`true`) |
| `KAFKA_URSA_STORAGE_BACKEND_TYPE` | Storage backend (`S3`) |
| `KAFKA_URSA_STORAGE_OXIA_SERVICE_URL` | Oxia connection URL |
| `KAFKA_URSA_STORAGE_S3_ENDPOINT` | S3 endpoint URL |
| `KAFKA_URSA_STORAGE_S3_BUCKET` | S3 bucket name |
| `KAFKA_URSA_STORAGE_S3_ACCESS_KEY` | S3 access key |
| `KAFKA_URSA_STORAGE_S3_SECRET_KEY` | S3 secret key |
| `KAFKA_URSA_STORAGE_S3_REGION` | S3 region |

**Note**: Environment variables use `_` for `.` in property names (e.g., `ursa.storage.enable` → `KAFKA_URSA_STORAGE_ENABLE`).

### Custom Image

To use a custom image:

```bash
IMAGE=myrepo/kafka-diskless:v1 docker compose up -d
```

## Testing Failover

Diskless storage enables automatic failover when a broker goes down:

```bash
# Stop broker 1
docker compose stop kafka-1

# Verify other brokers still serve the topic
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:39092 \
  --topic test-diskless \
  --from-beginning

# Restart broker 1
docker compose start kafka-1
```

## Cleanup

```bash
# Stop all services
docker compose down

# Remove all data volumes
docker compose down -v
```

## Troubleshooting

### Kafka fails to start

Check if Oxia and MinIO are healthy:

```bash
docker compose ps
docker compose logs oxia
docker compose logs minio
```

### Connection refused errors

Ensure the services have fully started:

```bash
# Wait for health checks
docker compose up -d --wait
```

### Permission errors

Ensure Docker version >= 20.10.4:

```bash
docker --version
```
