#!/bin/sh

set -e

TOPIC_NAME="movie-events"
PARTITIONS=3
REPLICATION_FACTOR=1

echo "Waiting for Kafka at $KAFKA_BROKER ..."
while ! kafka-topics --bootstrap-server "$KAFKA_BROKER" --list > /dev/null 2>&1; do
  echo "Kafka not ready, sleeping 2 seconds..."
  sleep 2
done
echo "Kafka is ready."

echo "Waiting for Schema Registry at $SCHEMA_REGISTRY_URL ..."
while ! curl -s "$SCHEMA_REGISTRY_URL/subjects" > /dev/null 2>&1; do
  echo "Schema Registry not ready, sleeping 2 seconds..."
  sleep 2
done
echo "Schema Registry is ready."

echo "Creating topic $TOPIC_NAME ..."
kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic "$TOPIC_NAME" \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

echo "Topic created."

echo "Registering Protobuf schema in Schema Registry..."
SCHEMA=$(cat /schemas/movie_event.proto | sed 's/"/\\"/g' | awk '{printf "%s\\n", $0}')

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{
    \"schemaType\": \"PROTOBUF\",
    \"schema\": \"$SCHEMA\"
  }" \
  "$SCHEMA_REGISTRY_URL/subjects/$TOPIC_NAME-value/versions"

echo "Schema registered (version 1)."

echo "Registering key schema (user_id string)..."
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{
    \"schemaType\": \"PROTOBUF\",
    \"schema\": \"syntax = \\\"proto3\\\"; message Key { string user_id = 1; }\"
  }" \
  "$SCHEMA_REGISTRY_URL/subjects/$TOPIC_NAME-key/versions" || echo "Key schema registration skipped"

echo "All done."