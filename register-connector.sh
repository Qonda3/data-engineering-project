#!/bin/bash
set -e

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="debezium-cdc-connector"
CONFIG_PATH="./volumes/config/debezium/source_config.json"

echo "⏳ Waiting for Kafka Connect REST API..."
until curl -sf $CONNECT_URL/connectors >/dev/null 2>&1; do
  sleep 3
done

echo "✅ Kafka Connect REST API is up."

if curl -sf $CONNECT_URL/connectors/$CONNECTOR_NAME >/dev/null 2>&1; then
  echo "ℹ️ Updating connector $CONNECTOR_NAME..."
  curl -s -X PUT -H "Content-Type: application/json" \
      --data @"$CONFIG_PATH" \
      $CONNECT_URL/connectors/$CONNECTOR_NAME/config
else
  echo "🚀 Creating connector $CONNECTOR_NAME..."
  curl -s -X POST -H "Content-Type: application/json" \
      --data @"$CONFIG_PATH" \
      $CONNECT_URL/connectors
fi

echo "🎉 Done. Connector is active."