#!/bin/bash
set -e

echo "Waiting for ClickHouse..."
until clickhouse-client --host clickhouse --port 9000 --user default --password clickhouse --query "SELECT 1" > /dev/null 2>&1; do
  echo "ClickHouse not ready, sleeping 2 seconds..."
  sleep 2
done
echo "ClickHouse is ready"

echo "Applying migrations..."
for f in /migrations/*.sql; do
  echo "Applying $f"
  clickhouse-client --host clickhouse --port 9000 --user default --password clickhouse --multiquery < "$f"
done
echo "Migrations complete"
