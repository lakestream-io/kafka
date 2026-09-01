#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
base="$here/docker-compose-lakehouse.yml"
demo="$here/docker-compose-lakehouse.demo.yml"
project="${COMPOSE_PROJECT_NAME:-kafka-ursa-lakehouse}"
kafka_image="${IMAGE:-kafka-diskless:latest}"
compactor_image="${COMPACTOR_IMAGE:-ursa-compactor:lakehouse-e2e}"
num_records="${NUM_RECORDS:-100}"
passed=false

for image in "$kafka_image" "$compactor_image"; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Required local image is missing: $image" >&2
    echo "Run URSA_STORAGE_DIR=/path/to/ursa-storage ./build-lakehouse-images.sh first." >&2
    exit 2
  fi
done

existing_containers="$(docker compose --project-name "$project" \
  -f "$base" -f "$demo" ps -aq)"
existing_volumes="$(docker volume ls \
  --filter "label=com.docker.compose.project=$project" --quiet)"
if [[ -n "$existing_containers" || -n "$existing_volumes" ]]; then
  echo "Compose project '$project' already has containers or volumes." >&2
  echo "Run 'make destroy-lakehouse' before starting a fresh E2E." >&2
  exit 2
fi

cleanup() {
  if [[ "$passed" == "true" && "${KEEP_RUNNING:-false}" != "true" ]]; then
    docker compose --project-name "$project" -f "$base" -f "$demo" \
      down -v --remove-orphans >/dev/null
  elif [[ "$passed" != "true" ]]; then
    echo "E2E failed; containers were left running for inspection." >&2
    echo "Logs: docker compose --project-name $project -f $base logs compactor polaris" >&2
  fi
}
trap cleanup EXIT

docker compose --project-name "$project" -f "$base" up -d \
  oxia minio minio-init polaris polaris-setup compactor kafka-1 kafka-2 kafka-3

docker compose --project-name "$project" -f "$base" -f "$demo" \
  run --rm kafka-consumer-check

attempt=1
last_query_output=""
while [[ "$attempt" -le 120 ]]; do
  if last_query_output="$(docker compose --project-name "$project" \
      -f "$base" -f "$demo" run --rm --no-deps duckdb-query-check 2>&1)"; then
    echo "$last_query_output"
    break
  fi
  echo "Iceberg table is not queryable yet; waiting... ($attempt/120)"
  attempt=$((attempt + 1))
  sleep 2
done

if [[ "$attempt" -gt 120 ]]; then
  echo "Timed out waiting for DuckDB to read the expected Iceberg rows." >&2
  echo "$last_query_output" >&2
  exit 1
fi

compactor_logs="$(docker compose --project-name "$project" -f "$base" logs --no-color compactor)"
materialization_count="$(printf '%s\n' "$compactor_logs" \
  | grep -F -c "Materializing [0,$num_records) of stream" || true)"
if [[ "$materialization_count" != "1" ]]; then
  echo "Expected exactly one materialization for [0,$num_records), found $materialization_count." >&2
  exit 1
fi
if grep -F -q "During compact error" <<< "$compactor_logs"; then
  echo "The compactor logged a task failure; inspect its logs before retrying." >&2
  exit 1
fi
if grep -F -q "Internal compaction task publisher is disabled" <<< "$compactor_logs"; then
  echo "The compactor unexpectedly disabled its internal task publisher." >&2
  exit 1
fi

passed=true
echo "E2E passed: Kafka -> Ursa WAL -> compaction -> Polaris/Iceberg -> DuckDB."
if [[ "${KEEP_RUNNING:-false}" == "true" ]]; then
  echo "The stack is still running under Compose project '$project'."
fi
